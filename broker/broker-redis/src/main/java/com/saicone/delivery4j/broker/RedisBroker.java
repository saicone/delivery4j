package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.Broker;
import com.saicone.delivery4j.util.LogFilter;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.util.SafeEncoder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Redis broker implementation to send data via publish and subscriptions.<br>
 * This kind of broker will encode any byte array as String and viceversa.<br>
 * Some operations made in this instance can fail due authentication errors,
 * so it requires the password as well.
 *
 * @author Rubenicos
 */
public class RedisBroker extends Broker {

    /**
     * Create a redis broker with provided parameters.
     *
     * @param host     the host to connect.
     * @param port     the port host.
     * @param password the password to validate authentication.
     * @param database the database number.
     * @param ssl      true to use SSL.
     * @return         a newly generated redis broker instance.
     */
    @NotNull
    public static RedisBroker of(@NotNull String host, int port, @NotNull String password, int database, boolean ssl) {
        final JedisClientConfig config = DefaultJedisClientConfig.builder()
                .password(password)
                .database(database)
                .ssl(ssl)
                .build();
        final RedisClient client = RedisClient.builder()
                .hostAndPort(host, port)
                .clientConfig(config)
                .build();
        return new RedisBroker(client);
    }

    private final RedisClient client;
    private final Bridge bridge;

    private long sleepTime = 8;
    private TimeUnit sleepUnit = TimeUnit.SECONDS;

    private Object aliveTask;

    /**
     * Constructs a redis broker with provided redis client.
     *
     * @param client the client to connect with.
     */
    public RedisBroker(@NotNull RedisClient client) {
        this(client, Bridge::new);
    }

    /**
     * Constructs a redis broker with provided redis client and bridge.
     *
     * @param client the client to connect with.
     * @param bridge the bridge supplier to receive messages from redis.
     */
    public RedisBroker(@NotNull RedisClient client, @NotNull Function<RedisBroker, Bridge> bridge) {
        this.client = client;
        this.bridge = bridge.apply(this);
    }

    @Override
    protected void onStart() {
        setEnabled(true);
        // Jedis connection is a blocking operation.
        // So new thread is needed to not block the current thread
        this.aliveTask = getExecutor().execute(this::alive);
    }

    @Override
    protected void onClose() {
        setEnabled(false);
        try {
            this.bridge.unsubscribe();
        } catch (Throwable ignored) { }
        try {
            this.client.close();
        } catch (Throwable ignored) { }
        try {
            this.client.getPool().close();
        } catch (Throwable ignored) { }
        if (this.aliveTask != null) {
            getExecutor().cancel(this.aliveTask);
        }
    }

    @Override
    protected void onSubscribe(@NotNull String... channels) {
        try {
            this.bridge.unsubscribe();
        } catch (Throwable ignored) { }
        if (this.aliveTask != null) {
            getExecutor().cancel(this.aliveTask);
        }
        this.aliveTask = getExecutor().execute(this::alive);
    }

    @Override
    protected void onUnsubscribe(@NotNull String... channels) {
        try {
            this.bridge.unsubscribe();
        } catch (Throwable ignored) { }
        if (this.aliveTask != null) {
            getExecutor().cancel(this.aliveTask);
        }
        this.aliveTask = getExecutor().execute(this::alive);
    }

    @Override
    public void send(@NotNull String channel, byte[] data) {
        this.client.publish(SafeEncoder.encode(channel), data);
    }

    /**
     * Set the reconnection interval that will be used on this redis broker instance.<br>
     * By default, 8 seconds is used.
     *
     * @param time the time to wait until reconnection is performed.
     * @param unit the unit that {@code time} is expressed in.
     */
    public void setReconnectionInterval(int time, @NotNull TimeUnit unit) {
        this.sleepTime = time;
        this.sleepUnit = unit;
    }

    /**
     * Get the current pool.
     *
     * @return a jedis pool object.
     */
    @NotNull
    public RedisClient getClient() {
        return client;
    }

    /**
     * Get the current bridge to receive messages.
     *
     * @return a bridge instance.
     */
    @NotNull
    public Bridge getBridge() {
        return bridge;
    }

    @SuppressWarnings("all")
    private void alive() {
        if (getSubscribedChannels().isEmpty()) {
            return;
        }
        boolean reconnected = false;
        while (isEnabled() && !Thread.interrupted() && this.client != null && !this.client.getPool().isClosed()) {
            try {
                if (reconnected) {
                    getLogger().log(LogFilter.INFO, "Redis connection is alive again");
                }
                // Subscribe channels and lock the thread
                this.client.subscribe(this.bridge, SafeEncoder.encodeMany(getSubscribedChannels().toArray(new String[0])));
            } catch (Throwable t) {
                // Thread was unlocked due error
                if (isEnabled()) {
                    if (reconnected) {
                        getLogger().log(LogFilter.WARNING, () -> "Redis connection dropped, automatic reconnection in " + this.sleepTime + " " + this.sleepUnit.name().toLowerCase() + "...", t);
                    }
                    try {
                        this.bridge.unsubscribe();
                    } catch (Throwable ignored) { }

                    // Make an instant subscribe if ocurrs any error on initialization
                    if (!reconnected) {
                        reconnected = true;
                    } else {
                        try {
                            Thread.sleep(this.sleepUnit.toMillis(this.sleepTime));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                } else {
                    return;
                }
            }
        }
    }

    /**
     * Bridge class to detect received messages from Redis database.
     */
    public static class Bridge extends BinaryJedisPubSub {

        private final Broker broker;

        /**
         * Constructs a bridge with provided broker.
         *
         * @param broker the broker to receive messages.
         */
        public Bridge(@NotNull Broker broker) {
            this.broker = broker;
        }

        @Override
        public void onMessage(byte[] channel, byte[] message) {
            final String channelString = SafeEncoder.encode(channel);
            if (this.broker.getSubscribedChannels().contains(channelString)) {
                try {
                    this.broker.receive(channelString, message);
                } catch (IOException e) {
                    this.broker.getLogger().log(LogFilter.WARNING, "Cannot process received message from channel '" + channelString + "'", e);
                }
            }
        }

        @Override
        public void onSubscribe(byte[] channel, int subscribedChannels) {
            this.broker.getLogger().log(LogFilter.INFO, "Redis subscribed to channel '" + SafeEncoder.encode(channel) + "'");
        }

        @Override
        public void onUnsubscribe(byte[] channel, int subscribedChannels) {
            this.broker.getLogger().log(LogFilter.INFO, "Redis unsubscribed from channel '" + SafeEncoder.encode(channel) + "'");
        }
    }
}

package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.Broker;
import com.saicone.delivery4j.util.LogFilter;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
public class LettuceBroker extends Broker {

    /**
     * Create a redis broker with provided URL string.<br>
     * The URL must be in {@code redis[s]://[[user][:password]@]host[:port][/database]} format.
     *
     * @param url the URL to connect.
     * @return    a newly generated redis broker instance.
     */
    @NotNull
    public static LettuceBroker simple(@NotNull String url) {
        return new LettuceBroker(RedisClient.create(url));
    }

    /**
     * Create a redis broker with provided URI.<br>
     * The URI must be in {@code redis[s]://[[user][:password]@]host[:port][/database]} format.
     *
     * @param uri the URI to connect.
     * @return    a newly generated redis broker instance.
     */
    @NotNull
    public static LettuceBroker simple(@NotNull URI uri) {
        return new LettuceBroker(RedisClient.create(RedisURI.create(uri)));
    }

    /**
     * Create a redis broker with provided parameters.
     *
     * @param address  the address to connect, must be in {@code host:port} format.
     * @param password the password to validate authentication.
     * @param database the database number.
     * @param ssl      true to use SSL.
     * @return         a newly generated redis broker instance.
     */
    @NotNull
    public static LettuceBroker simple(@NotNull String address, @NotNull String password, @Nullable Integer database, boolean ssl) {
        final String[] parts = address.split(":");
        final RedisURI.Builder builder = RedisURI.builder()
                .withHost(parts[0])
                .withPort(Integer.parseInt(parts[1]))
                .withPassword(password)
                .withSsl(ssl);
        if (database != null) {
            builder.withDatabase(database);
        }

        return new LettuceBroker(RedisClient.create(builder.build()));
    }

    private final RedisClient client;
    private StatefulRedisPubSubConnection<String, byte[]> pubSubConnection;
    private final Listener listener;

    private long sleepTime = 8;
    private TimeUnit sleepUnit = TimeUnit.SECONDS;

    /**
     * Constructs a redis broker with provided redis client.
     *
     * @param client the client to connect with.
     */
    public LettuceBroker(@NotNull RedisClient client) {
        this(client, Listener::new);
    }

    /**
     * Constructs a redis broker with provided redis client and bridge.
     *
     * @param client the client to connect with.
     * @param bridge the bridge supplier to receive messages from redis.
     */
    public LettuceBroker(@NotNull RedisClient client, @NotNull Function<LettuceBroker, Listener> bridge) {
        this.client = client;
        this.listener = bridge.apply(this);
    }

    @Override
    protected void onStart() {
        setEnabled(true);

        this.pubSubConnection = this.client.connectPubSub(RedisCodec.of(new StringCodec(StandardCharsets.UTF_8), new ByteArrayCodec()));
        this.pubSubConnection.addListener(this.listener);
        this.listener.start();
    }

    @Override
    protected void onClose() {
        setEnabled(false);

        this.listener.close();
        this.pubSubConnection.close();
        this.client.shutdown();
    }

    @Override
    protected void onSubscribe(@NotNull String... channels) {
        this.listener.close();
        this.listener.start();
    }

    @Override
    protected void onUnsubscribe(@NotNull String... channels) {
        this.listener.close();
        this.listener.start();
    }

    @Override
    public void send(@NotNull String channel, byte[] data) {
        this.pubSubConnection.sync().publish(channel, data);
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
     * Check if the broker is available to send and receive messages. This method is used to detect if
     * the connection is alive or not, so it can be used to perform reconnections.
     *
     * @return true if the broker is available, false otherwise.
     */
    public boolean isAvailable() {
        return isEnabled() && !Thread.interrupted() && !isConnectionClosed();
    }

    /**
     * Check if the lettuce connection is closed or not. This method is used to detect if
     * the connection is alive or not, so it can be used to perform reconnections.
     *
     * @return true if the lettuce connection is closed, false otherwise.
     */
    public boolean isConnectionClosed() {
        return this.pubSubConnection == null || !this.pubSubConnection.isOpen();
    }

    /**
     * Get the current client.
     *
     * @return a redis client object.
     */
    @NotNull
    public RedisClient getClient() {
        return client;
    }

    /**
     * Get the current pubsub connection.
     *
     * @return a stateful redis pubsub connection object.
     */
    @NotNull
    public StatefulRedisPubSubConnection<String, byte[]> getPubSubConnection() {
        return pubSubConnection;
    }

    /**
     * Get the current bridge to receive messages.
     *
     * @return a bridge instance.
     */
    @NotNull
    public LettuceBroker.Listener getListener() {
        return listener;
    }

    /**
     * Get the current reconnection interval time.
     *
     * @return the time to wait until reconnection is performed.
     */
    public long getSleepTime() {
        return sleepTime;
    }

    /**
     * Get the current reconnection interval unit.
     *
     * @return the unit that reconnection time is expressed in.
     */
    @NotNull
    public TimeUnit getSleepUnit() {
        return sleepUnit;
    }

    /**
     * Bridge class to detect received messages from Redis database.
     */
    public static class Listener implements RedisPubSubListener<String, byte[]> {

        private final LettuceBroker broker;

        private Object lockedTask;
        private boolean reconnected;

        /**
         * Constructs a bridge with provided broker.
         *
         * @param broker the broker to receive messages.
         */
        public Listener(@NotNull LettuceBroker broker) {
            this.broker = broker;
        }

        /**
         * Start the subscription to channels. This method will subscribe to all channels that are
         * currently subscribed in the broker, so it will receive messages from those channels.
         */
        @ApiStatus.Internal
        public void start() {
            // Only subscribe if there is any channel to listen (otherwise this will cause a rare exception)
            if (!this.broker.getSubscribedChannels().isEmpty()) {
                this.lockedTask = this.broker.getExecutor().execute(this::subscribe);
            }
        }

        /**
         * Close the subscription to channels. This method will unsubscribe from all channels that are
         * currently subscribed in the broker, so it will stop receiving messages from those channels.
         */
        @ApiStatus.Internal
        public void close() {
            unsubscribe0();

            if (this.lockedTask != null) {
                this.broker.getExecutor().cancel(this.lockedTask);
            }
        }

        /**
         * Subscribe to channels and lock the thread until an error occurs or the subscription is closed.
         * This method will subscribe to all channels that are currently subscribed.
         */
        @ApiStatus.Internal
        @Blocking
        public void subscribe() {
            if (this.broker.isAvailable()) {
                try {
                    if (this.reconnected) {
                        this.broker.getLogger().log(LogFilter.INFO, "Redis connection is alive again");
                    }
                    // Subscribe channels and lock the thread
                    this.broker.getPubSubConnection().sync().subscribe(this.broker.getSubscribedChannels().toArray(new String[0]));
                } catch (Throwable t) {
                    // Thread was unlocked due error, lets try to reconnect
                    final boolean sleep = this.reconnected;
                    this.reconnected = true;
                    reconnect(t, sleep);
                }
            }
        }

        /**
         * Unsubscribe from channels and unlock the thread. This method will unsubscribe from all channels
         * that are currently subscribed.
         */
        @ApiStatus.Internal
        public void unsubscribe0() {
            try {
                this.broker.getPubSubConnection().sync().unsubscribe();
            } catch (Throwable t) {
                this.broker.getLogger().log(LogFilter.DEBUG, "There is an error while unsubscribing redis pubsub", t);
            }
        }

        private void reconnect(@NotNull Throwable t, boolean sleep) {
            if (!this.broker.isAvailable()) {
                return;
            }

            if (sleep) {
                this.broker.getLogger().log(LogFilter.WARNING, () -> "Redis connection dropped, automatic reconnection in " + this.broker.getSleepTime() + " " + this.broker.getSleepUnit().name().toLowerCase() + "...", t);
            } else {
                this.broker.getLogger().log(LogFilter.WARNING, "Redis listener got unlocked, making an instant reconnection...", t);
            }

            unsubscribe0();

            if (sleep) {
                this.broker.getExecutor().execute(this::start, this.broker.getSleepTime(), this.broker.getSleepUnit());
            } else {
                start();
            }
        }

        @Override
        public void message(String channel, byte[] message) {
            if (this.broker.getSubscribedChannels().contains(channel)) {
                try {
                    this.broker.receive(channel, message);
                } catch (IOException e) {
                    this.broker.getLogger().log(LogFilter.WARNING, "Cannot process received message from channel '" + channel + "'", e);
                }
            }
        }

        @Override
        public void message(String pattern, String channel, byte[] message) {
            this.message(channel, message);
        }

        @Override
        public void subscribed(String channel, long count) {
            this.broker.getLogger().log(LogFilter.INFO, "Redis subscribed to channel '" + channel + "'");
        }

        @Override
        public void psubscribed(String pattern, long count) {
            this.broker.getLogger().log(LogFilter.INFO, "Redis subscribed to pattern '" + pattern + "'");
        }

        @Override
        public void unsubscribed(String channel, long count) {
            this.broker.getLogger().log(LogFilter.INFO, "Redis unsubscribed from channel '" + channel + "'");
        }

        @Override
        public void punsubscribed(String pattern, long count) {
            this.broker.getLogger().log(LogFilter.INFO, "Redis unsubscribed from pattern '" + pattern + "'");
        }
    }
}

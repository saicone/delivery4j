package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.Broker;
import com.saicone.delivery4j.util.LogFilter;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.util.SafeEncoder;

import java.io.IOException;
import java.net.URI;
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
public class JedisBroker extends Broker {

    /**
     * Create a redis broker with provided URL string.<br>
     * The URL must be in {@code redis[s]://[[user][:password]@]host[:port][/database]} format.
     *
     * @param url the URL to connect.
     * @return    a newly generated redis broker instance.
     */
    @NotNull
    public static JedisBroker simple(@NotNull String url) {
        return new JedisBroker(RedisClient.create(url));
    }

    /**
     * Create a redis broker with provided URI.<br>
     * The URI must be in {@code redis[s]://[[user][:password]@]host[:port][/database]} format.
     *
     * @param uri the URI to connect.
     * @return    a newly generated redis broker instance.
     */
    @NotNull
    public static JedisBroker simple(@NotNull URI uri) {
        return new JedisBroker(RedisClient.create(uri));
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
    public static JedisBroker simple(@NotNull String address, @NotNull String password, @Nullable Integer database, boolean ssl) {
        final DefaultJedisClientConfig.Builder builder = DefaultJedisClientConfig.builder()
                .password(password)
                .ssl(ssl);
        if (database != null) {
            builder.database(database);
        }

        final RedisClient client = RedisClient.builder()
                .hostAndPort(HostAndPort.from(address))
                .clientConfig(builder.build())
                .build();

        return new JedisBroker(client);
    }

    private final UnifiedJedis jedis;
    private final Listener listener;

    private long sleepTime = 8;
    private TimeUnit sleepUnit = TimeUnit.SECONDS;

    /**
     * Constructs a redis broker with provided redis client.
     *
     * @param jedis the client to connect with.
     */
    public JedisBroker(@NotNull UnifiedJedis jedis) {
        this(jedis, Listener::new);
    }

    /**
     * Constructs a redis broker with provided redis client and bridge.
     *
     * @param jedis the client to connect with.
     * @param bridge the bridge supplier to receive messages from redis.
     */
    public JedisBroker(@NotNull UnifiedJedis jedis, @NotNull Function<JedisBroker, Listener> bridge) {
        this.jedis = jedis;
        this.listener = bridge.apply(this);
    }

    @Override
    protected void onStart() {
        setEnabled(true);

        this.listener.start();
    }

    @Override
    @SuppressWarnings("deprecation")
    protected void onClose() {
        setEnabled(false);

        this.listener.close();

        try {
            this.jedis.close();
        } catch (Throwable t) {
            getLogger().log(LogFilter.DEBUG, "There is an error while closing jedis connection", t);
        }

        try {
            if (this.jedis instanceof RedisClient) {
                ((RedisClient) this.jedis).getPool().close();
            } else if (this.jedis instanceof RedisClusterClient) {
                ((RedisClusterClient) this.jedis).getClusterNodes().forEach((key, pool) -> pool.close());
            } else if (this.jedis instanceof JedisPooled) {
                // deprecated
                ((JedisPooled) this.jedis).getPool().close();
            } else if (this.jedis instanceof JedisCluster) {
                // deprecated
                ((JedisCluster) this.jedis).getClusterNodes().forEach((key, pool) -> pool.close());
            }
        } catch (Throwable t) {
            getLogger().log(LogFilter.DEBUG, "There is an error while closing pool connection", t);
        }
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
        this.jedis.publish(SafeEncoder.encode(channel), data);
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
        return isEnabled() && !isJedisClosed();
    }

    /**
     * Check if the jedis connection is closed or not. This method is used to detect if
     * the connection is alive or not, so it can be used to perform reconnections.
     *
     * @return true if the jedis connection is closed, false otherwise.
     */
    @SuppressWarnings("deprecation")
    public boolean isJedisClosed() {
        if (this.jedis instanceof RedisClient) {
            return ((RedisClient) this.jedis).getPool().isClosed();
        } else if (this.jedis instanceof RedisClusterClient) {
            return ((RedisClusterClient) this.jedis).getClusterNodes().isEmpty();
        } else if (this.jedis instanceof JedisPooled) {
            // deprecated
            return ((JedisPooled) this.jedis).getPool().isClosed();
        } else if (this.jedis instanceof JedisCluster) {
            // deprecated
            return ((JedisCluster) this.jedis).getClusterNodes().isEmpty();
        } else {
            return false;
        }
    }

    /**
     * Get the current pool.
     *
     * @return a jedis pool object.
     */
    @NotNull
    public UnifiedJedis getJedis() {
        return jedis;
    }

    /**
     * Get the current bridge to receive messages.
     *
     * @return a bridge instance.
     */
    @NotNull
    public JedisBroker.Listener getListener() {
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
    public static class Listener extends BinaryJedisPubSub {

        private final JedisBroker broker;

        private Object lockedTask;
        private boolean reconnected;

        /**
         * Constructs a bridge with provided broker.
         *
         * @param broker the broker to receive messages.
         */
        public Listener(@NotNull JedisBroker broker) {
            this.broker = broker;
        }

        /**
         * Start the subscription to channels. This method will subscribe to all channels that are
         * currently subscribed in the broker, so it will receive messages from those channels.
         */
        @ApiStatus.Internal
        public void start() {
            // Only subscribe it there is any channel to listen (otherwise this will cause a rare exception)
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
                    this.broker.getJedis().subscribe(this, SafeEncoder.encodeMany(this.broker.getSubscribedChannels().toArray(new String[0])));
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
                this.unsubscribe();
            } catch (Throwable t) {
                this.broker.getLogger().log(LogFilter.DEBUG, "There is an error while unsubscribing jedis pubsub", t);
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

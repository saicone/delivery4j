package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.Broker;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Redis broker implementation to send data via publish and subscriptions.<br>
 * This kind of broker will encode any byte array as String and viceversa.<br>
 * Some operations made in this instance can fail due authentication errors,
 * so it requires the password as well.
 *
 * @author Rubenicos
 */
public class RedisBroker extends Broker {

    private final JedisPool pool;
    private final Supplier<String> password;
    private final Bridge bridge;

    private long sleepTime = 8;
    private TimeUnit sleepUnit = TimeUnit.SECONDS;

    private Object aliveTask;

    /**
     * Create a redis broker with provided url.<br>
     * This method will try to extract any password from provided url.
     *
     * @param url the URL to connect with.
     * @return    a newly generated redis broker instance.
     */
    @NotNull
    public static RedisBroker of(@NotNull String url) {
        String password = "";
        if (url.contains("@")) {
            final String s = url.substring(0, url.lastIndexOf("@"));
            if (s.contains(":")) {
                password = s.substring(s.lastIndexOf(":") + 1);
            }
        }
        try {
            return of(new URI(url), password);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a redis broker with provided url and password.
     *
     * @param uri      the URL object to connect with.
     * @param password the password to validate authentication.
     * @return         a newly generated redis broker instance.
     */
    @NotNull
    public static RedisBroker of(@NotNull URI uri, @NotNull String password) {
        return new RedisBroker(new JedisPool(uri), password);
    }

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
        return new RedisBroker(new JedisPool(new JedisPoolConfig(), host, port, Protocol.DEFAULT_TIMEOUT, password, database, ssl), password);
    }

    /**
     * Constructs a redis broker with provided pool and password.
     *
     * @param pool     the pool to connect with.
     * @param password the used redis password.
     */
    public RedisBroker(@NotNull JedisPool pool, @NotNull String password) {
        this.pool = pool;
        this.password = password(password);
        this.bridge = new Bridge();
    }

    /**
     * Constructs a redis broker with provided parameters.
     *
     * @param pool     the pool to connect with.
     * @param password the used redis password.
     * @param bridge   the bridge to receive messages from redis.
     */
    public RedisBroker(@NotNull JedisPool pool, @NotNull String password, @NotNull Bridge bridge) {
        this.pool = pool;
        this.password = password(password);
        this.bridge = bridge;
    }

    @NotNull
    private Supplier<String> password(@NotNull String password) {
        return () -> {
            final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
            for (int i = 2; i < stack.length; i++) {
                if (stack[i].getClassName().equals(RedisBroker.class.getName())) {
                    return password;
                }
            }

            throw new SecurityException("Redis password is only accessible from Redis broker instance");
        };
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
            this.pool.destroy();
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
    protected void onSend(@NotNull String channel, byte[] data) throws IOException {
        try (Jedis jedis = this.pool.getResource()) {
            final String message = getCodec().encode(data);
            try {
                jedis.publish(channel, message);
            } catch (JedisDataException e) {
                // Fix Java +16 error
                if (e.getMessage().contains("NOAUTH")) {
                    jedis.auth(this.password.get());
                    jedis.publish(channel, message);
                } else {
                    throw new IOException(e);
                }
            }
        }
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
    public JedisPool getPool() {
        return pool;
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
        while (isEnabled() && !Thread.interrupted() && this.pool != null && !this.pool.isClosed()) {
            try (Jedis jedis = this.pool.getResource()) {
                if (reconnected) {
                    getLogger().log(3, "Redis connection is alive again");
                }
                // Subscribe channels and lock the thread
                jedis.subscribe(this.bridge, getSubscribedChannels().toArray(new String[0]));
            } catch (Throwable t) {
                // Thread was unlocked due error
                if (isEnabled()) {
                    if (reconnected) {
                        getLogger().log(2, () -> "Redis connection dropped, automatic reconnection in " + this.sleepTime + " " + this.sleepUnit.name().toLowerCase() + "...", t);
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
    public class Bridge extends JedisPubSub {

        @Override
        public void onMessage(String channel, String message) {
            if (channel != null && RedisBroker.this.getSubscribedChannels().contains(channel) && message != null) {
                try {
                    receive(channel, getCodec().decode(message));
                } catch (IOException e) {
                    getLogger().log(2, "Cannot process received message from channel '" + channel + "'", e);
                }
            }
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            getLogger().log(3, "Redis subscribed to channel '" + channel + "'");
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            getLogger().log(3, "Redis unsubscribed from channel '" + channel + "'");
        }
    }
}

package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.Broker;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;
import java.net.URI;

/**
 * Redis integration for data delivery.
 *
 * @author Rubenicos
 */
public class RedisBroker extends Broker<RedisBroker> {

    private final JedisPool pool;
    private final String password;
    private final Bridge bridge;

    private Object aliveTask;

    /**
     * Create a RedisDelivery client with provided parameters.
     *
     * @param url the URL to connect with.
     * @return    new RedisDelivery instance.
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
     * Create a RedisDelivery client with provided parameters.
     *
     * @param uri      the URL object to connect with.
     * @param password the password to validate authentication.
     * @return         new RedisDelivery instance.
     */
    @NotNull
    public static RedisBroker of(@NotNull URI uri, @NotNull String password) {
        return new RedisBroker(new JedisPool(uri), password);
    }

    /**
     * Create a RedisDelivery client with provided parameters.
     *
     * @param host     the host to connect.
     * @param port     the port host.
     * @param password the password to validate authentication.
     * @param database the database number.
     * @param ssl      true to use SSL.
     * @return         new RedisDelivery instance.
     */
    @NotNull
    public static RedisBroker of(@NotNull String host, int port, @NotNull String password, int database, boolean ssl) {
        return new RedisBroker(new JedisPool(new JedisPoolConfig(), host, port, Protocol.DEFAULT_TIMEOUT, password, database, ssl), password);
    }

    /**
     * Constructs a RedisDelivery with provided parameters.
     *
     * @param pool     the pool to connect with.
     * @param password the used redis password.
     */
    public RedisBroker(@NotNull JedisPool pool, @NotNull String password) {
        this.pool = pool;
        this.password = password;
        this.bridge = new Bridge();
    }

    /**
     * Constructs a RedisDelivery with provided parameters.
     *
     * @param pool     the pool to connect with.
     * @param password the used redis password.
     * @param bridge   the bridge to receive messages from redis.
     */
    public RedisBroker(@NotNull JedisPool pool, @NotNull String password, @NotNull Bridge bridge) {
        this.pool = pool;
        this.password = password;
        this.bridge = bridge;
    }

    @Override
    protected void onStart() {
        setEnabled(true);
        // Jedis connection is a blocking operation.
        // So new thread is needed to not block the main thread
        aliveTask = getExecutor().execute(this::alive);
    }

    @Override
    protected void onClose() {
        setEnabled(false);
        try {
            bridge.unsubscribe();
        } catch (Throwable ignored) { }
        pool.destroy();
    }

    @Override
    protected void onSubscribe(@NotNull String... channels) {
        try {
            bridge.unsubscribe();
        } catch (Throwable ignored) { }
        if (aliveTask != null) {
            getExecutor().cancel(aliveTask);
        }
        aliveTask = getExecutor().execute(this::alive);
    }

    @Override
    protected void onUnsubscribe(@NotNull String... channels) {
        try {
            bridge.unsubscribe();
        } catch (Throwable ignored) { }
        if (aliveTask != null) {
            getExecutor().cancel(aliveTask);
        }
        aliveTask = getExecutor().execute(this::alive);
    }

    @Override
    protected void onSend(@NotNull String channel, byte[] data) throws IOException {
        try (Jedis jedis = pool.getResource()) {
            final String message = getCodec().encode(data);
            try {
                jedis.publish(channel, message);
            } catch (JedisDataException e) {
                // Fix Java +16 error
                if (e.getMessage().contains("NOAUTH")) {
                    jedis.auth(password);
                    jedis.publish(channel, message);
                } else {
                    throw new IOException(e);
                }
            }
        }
    }

    @Override
    protected @NotNull RedisBroker get() {
        return this;
    }

    /**
     * The current pool.
     *
     * @return a jedis pool object.
     */
    @NotNull
    public JedisPool getPool() {
        return pool;
    }

    /**
     * The current password for authentication.
     *
     * @return a String password.
     */
    @NotNull
    public String getPassword() {
        return password;
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
        boolean reconnected = false;
        while (isEnabled() && !Thread.interrupted() && pool != null && !pool.isClosed()) {
            try (Jedis jedis = pool.getResource()) {
                if (reconnected) {
                    getLogger().log(3, "Redis connection is alive again");
                }
                // Subscribe channels and lock the thread
                jedis.subscribe(bridge, getSubscribedChannels().toArray(new String[0]));
            } catch (Throwable t) {
                // Thread was unlocked due error
                if (isEnabled()) {
                    if (reconnected) {
                        getLogger().log(2, "Redis connection dropped, automatic reconnection in 8 seconds...\n" + t.getMessage());
                    }
                    try {
                        bridge.unsubscribe();
                    } catch (Throwable ignored) { }

                    // Make an instant subscribe if ocurrs any error on initialization
                    if (!reconnected) {
                        reconnected = true;
                    } else {
                        try {
                            Thread.sleep(8000);
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

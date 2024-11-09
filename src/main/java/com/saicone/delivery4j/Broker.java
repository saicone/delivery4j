package com.saicone.delivery4j;

import com.saicone.delivery4j.util.ByteCodec;
import com.saicone.delivery4j.util.DelayedExecutor;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * Delivery client abstract class with common methods to transfer data and initialize any connection.
 *
 * @author Rubenicos
 */
public abstract class Broker<T extends Broker<T>> {

    private ChannelConsumer<byte[]> consumer = (channel, data) -> {};
    private ByteCodec<String> codec = ByteCodec.BASE64;
    private DelayedExecutor<?> executor = DelayedExecutor.JAVA;
    private Logger logger = Logger.of(this.getClass());

    /**
     * A Set of subscribed channels IDs.
     */
    private final Set<String> subscribedChannels = new HashSet<>();
    /**
     * Boolean object to mark the current delivery client as enabled or disabled.
     */
    private boolean enabled = false;

    /**
     * Method to run when client starts
     */
    protected void onStart() {
    }

    /**
     * Method to run when client stops.
     */
    protected void onClose() {
    }

    /**
     * Method to run when client is subscribed to new channels.
     *
     * @param channels the channels IDs.
     */
    protected void onSubscribe(@NotNull String... channels) {
    }

    /**
     * Method to run when client is unsubscribed to new channels.
     *
     * @param channels the channels IDs.
     */
    protected void onUnsubscribe(@NotNull String... channels) {
    }

    /**
     * Method to run when byte data is being sent to client.
     *
     * @param channel the channel ID.
     * @param data    the byte array data to send.
     */
    protected void onSend(@NotNull String channel, byte[] data) throws IOException {
    }

    /**
     * Method to run when byte data was received from client.
     *
     * @param channel the channel ID.
     * @param data    the received byte array data.
     */
    protected void onReceive(@NotNull String channel, byte[] data) throws IOException {
    }

    @NotNull
    protected abstract T get();

    @NotNull
    public ChannelConsumer<byte[]> getConsumer() {
        return consumer;
    }

    @NotNull
    public ByteCodec<String> getCodec() {
        return codec;
    }

    @NotNull
    @SuppressWarnings("unchecked")
    public DelayedExecutor<Object> getExecutor() {
        return (DelayedExecutor<Object>) executor;
    }

    @NotNull
    public Logger getLogger() {
        return logger;
    }

    /**
     * Get the subscribed channels.
     *
     * @return a Set of channels IDs.
     */
    @NotNull
    public Set<String> getSubscribedChannels() {
        return subscribedChannels;
    }

    /**
     * Get the current client status.
     *
     * @return true if the client is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    @NotNull
    @Contract("_ -> this")
    public T consumer(@NotNull ChannelConsumer<byte[]> consumer) {
        this.consumer = consumer;
        return get();
    }

    @NotNull
    @Contract("_ -> this")
    public T codec(@NotNull ByteCodec<String> codec) {
        this.codec = codec;
        return get();
    }

    @NotNull
    @Contract("_ -> this")
    public T executor(@NotNull DelayedExecutor<?> executor) {
        this.executor = executor;
        return get();
    }

    @NotNull
    @Contract("_ -> this")
    public T setLogger(@NotNull Logger logger) {
        this.logger = logger;
        return get();
    }

    /**
     * Set client status.
     *
     * @param enabled true for enabled, false otherwise.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Start the delivery client.
     */
    public void start() {
        close();
        onStart();
    }

    /**
     * Stop the delivery client.
     */
    public void close() {
        if (isEnabled()) {
            setEnabled(false);
            onClose();
        }
    }

    /**
     * Clear all subscribed channels from delivery client.
     */
    public void clear() {
        getSubscribedChannels().clear();
    }

    /**
     * Subscribe client into provided channels IDs.
     *
     * @param channels the channels to register.
     * @return         true if any channel was added, false if all the provided channels are already registered.
     */
    public boolean subscribe(@NotNull String... channels) {
        final List<String> list = new ArrayList<>();
        for (String channel : channels) {
            if (getSubscribedChannels().add(channel)) {
                list.add(channel);
            }
        }
        if (list.isEmpty()) {
            return false;
        }
        onSubscribe(list.toArray(new String[0]));
        return true;
    }

    /**
     * Unsubscribe client from provided channels IDs.
     *
     * @param channels the channels to unregister.
     * @return         true if any channel was removed, false if all the provided channels are already unregistered.
     */
    public boolean unsubscribe(@NotNull String... channels) {
        final List<String> list = new ArrayList<>();
        for (String channel : channels) {
            if (getSubscribedChannels().remove(channel)) {
                list.add(channel);
            }
        }
        if (list.isEmpty()) {
            return false;
        }
        onUnsubscribe(list.toArray(new String[0]));
        return true;
    }

    /**
     * Send byte data array to provided channel.
     *
     * @param channel the channel ID.
     * @param data    the data to send.
     */
    public void send(@NotNull String channel, byte[] data) throws IOException {
        onSend(channel, data);
    }

    /**
     * Receive byte array from provided channel.
     *
     * @param channel the channel ID.
     * @param data    the data to receive.
     */
    public void receive(@NotNull String channel, byte[] data) throws IOException {
        getConsumer().accept(channel, data);
        onReceive(channel, data);
    }

    public interface Logger {

        boolean DEBUG = "true".equals(System.getProperty("saicone.delivery4j.debug"));

        @NotNull
        static Logger of(@NotNull Class<?> clazz) {
            try {
                Class.forName("org.apache.logging.log4j.Logger");
                return Class.forName("com.saicone.delivery4j.log.Log4jLogger")
                        .asSubclass(Logger.class)
                        .getDeclaredConstructor(Class.class)
                        .newInstance(clazz);
            } catch (Throwable ignored) { }

            try {
                Class.forName("org.slf4j.Logger");
                return Class.forName("com.saicone.delivery4j.log.Slf4jLogger")
                        .asSubclass(Logger.class)
                        .getDeclaredConstructor(Class.class)
                        .newInstance(clazz);
            } catch (Throwable ignored) { }

            return new Logger() {
                private final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(clazz.getName());

                private void log(int level, @NotNull Consumer<Level> consumer) {
                    switch (level) {
                        case 1:
                            consumer.accept(Level.SEVERE);
                            break;
                        case 2:
                            consumer.accept(Level.WARNING);
                            break;
                        case 3:
                            consumer.accept(Level.INFO);
                            break;
                        case 4:
                        default:
                            if (DEBUG) {
                                consumer.accept(Level.INFO);
                            }
                            break;
                    }
                }

                @Override
                public void log(int level, @NotNull String msg) {
                    log(level, lvl -> this.logger.log(lvl, msg));
                }

                @Override
                public void log(int level, @NotNull String msg, @NotNull Throwable throwable) {
                    log(level, lvl -> this.logger.log(lvl, msg, throwable));
                }

                @Override
                public void log(int level, @NotNull Supplier<String> msg) {
                    log(level, lvl -> this.logger.log(lvl, msg));
                }

                @Override
                public void log(int level, @NotNull Supplier<String> msg, @NotNull Throwable throwable) {
                    log(level, lvl -> this.logger.log(lvl, throwable, msg));
                }
            };
        }

        void log(int level, @NotNull String msg);

        void log(int level, @NotNull String msg, @NotNull Throwable throwable);

        default void log(int level, @NotNull Supplier<String> msg) {
            log(level, msg.get());
        }

        default void log(int level, @NotNull Supplier<String> msg, @NotNull Throwable throwable) {
            log(level, msg.get(), throwable);
        }
    }
}

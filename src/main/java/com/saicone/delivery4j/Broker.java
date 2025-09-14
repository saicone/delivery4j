package com.saicone.delivery4j;

import com.saicone.delivery4j.util.ByteCodec;
import com.saicone.delivery4j.util.TaskExecutor;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * Represents an object that can transfer byte-array data across channels.<br>
 * In popular terms, this is a globalized producer that deliver/publish temporary
 * data to multiple consumers using topics or queues (depending on the implementation)
 * and can also consume the data itself by subscribing to channels.
 *
 * @author Rubenicos
 */
public abstract class Broker {

    private ChannelConsumer<byte[]> consumer = (channel, data) -> {};
    private ByteCodec<String> codec = ByteCodec.BASE64;
    private TaskExecutor<?> executor = TaskExecutor.JAVA;
    private Logger logger = Logger.of(this.getClass());

    private final Set<String> subscribedChannels = new HashSet<>();
    private boolean enabled = false;

    /**
     * Method to run when broker starts
     */
    protected void onStart() {
    }

    /**
     * Method to run when broker closes.
     */
    protected void onClose() {
    }

    /**
     * Method to run when broker is being subscribed to new channels.
     *
     * @param channels the channels name.
     */
    protected void onSubscribe(@NotNull String... channels) {
    }

    /**
     * Method to run when broker is being unsubscribed from channels.
     *
     * @param channels the channels name.
     */
    protected void onUnsubscribe(@NotNull String... channels) {
    }

    /**
     * Method to run when byte data is being sent to broker.
     *
     * @param channel the channel name.
     * @param data    the byte array data to send.
     * @throws IOException if any error occurs while sending the data.
     */
    protected abstract void onSend(@NotNull String channel, byte[] data) throws IOException;

    /**
     * Method to run when byte data was received from broker.
     *
     * @param channel the channel name.
     * @param data    the received byte array data.
     * @throws IOException if any error occurs while receiving the data.
     */
    protected void onReceive(@NotNull String channel, byte[] data) throws IOException {
    }

    /**
     * Get the current channel consumer.
     *
     * @return a channel consumer that accept a channel name with byte-array data.
     */
    @NotNull
    public ChannelConsumer<byte[]> getConsumer() {
        return consumer;
    }

    /**
     * Get the current byte codec.
     *
     * @return a byte codec that convert bytes into/from String.
     */
    @NotNull
    public ByteCodec<String> getCodec() {
        return codec;
    }

    /**
     * Get the current delayed executor.
     *
     * @return a delayed executor.
     */
    @NotNull
    @SuppressWarnings("unchecked")
    public TaskExecutor<Object> getExecutor() {
        return (TaskExecutor<Object>) executor;
    }

    /**
     * Get the current logger.
     *
     * @return a logger that print information about broker operations.
     */
    @NotNull
    public Logger getLogger() {
        return logger;
    }

    /**
     * Get the subscribed channels.
     *
     * @return a set of channels names.
     */
    @NotNull
    public Set<String> getSubscribedChannels() {
        return subscribedChannels;
    }

    /**
     * Get the current broker status.
     *
     * @return true if the broker is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Replace the current channel consumer.
     *
     * @param consumer the channel consumer to set.
     */
    public void setConsumer(@NotNull ChannelConsumer<byte[]> consumer) {
        this.consumer = consumer;
    }

    /**
     * Replace the current byte codec.
     *
     * @param codec the byte codec to set.
     */
    public void setCodec(@NotNull ByteCodec<String> codec) {
        this.codec = codec;
    }

    /**
     * Replace the current delayed executor.
     *
     * @param executor the delayed executor to set.
     */
    public void setExecutor(@NotNull TaskExecutor<?> executor) {
        this.executor = executor;
    }

    /**
     * Replace the current logger.
     *
     * @param logger the logger to set.
     */
    public void setLogger(@NotNull Logger logger) {
        this.logger = logger;
    }

    /**
     * Set broker status.
     *
     * @param enabled true for enabled, false otherwise.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Start the broker connection.
     */
    public void start() {
        close();
        onStart();
    }

    /**
     * Close the broker connection.
     */
    public void close() {
        if (isEnabled()) {
            setEnabled(false);
            onClose();
        }
    }

    /**
     * Clear all subscribed channels from broker.
     */
    public void clear() {
        getSubscribedChannels().clear();
    }

    /**
     * Subscribe broker into provided channels names.<br>
     * Any repeated channel will be ignored.
     *
     * @param channels the channels to register.
     * @return         true if any channel was added, false if all the provided channels are already registered.
     */
    public boolean subscribe(@NotNull String... channels) {
        final Set<String> list = new HashSet<>();
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
     * Unsubscribe broker from provided channels names.<br>
     * Any repeated channel will be ignored.
     *
     * @param channels the channels to unregister.
     * @return         true if any channel was removed, false if all the provided channels are already unregistered.
     */
    public boolean unsubscribe(@NotNull String... channels) {
        final Set<String> list = new HashSet<>();
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
     * @param channel the channel name.
     * @param data    the data to send.
     * @throws IOException if anny error occurs while sending the data.
     */
    public void send(@NotNull String channel, byte[] data) throws IOException {
        onSend(channel, data);
    }

    /**
     * Receive byte array from provided channel.
     *
     * @param channel the channel name.
     * @param data    the data to receive.
     * @throws IOException if any error occurs while receiving the data.
     */
    public void receive(@NotNull String channel, byte[] data) throws IOException {
        getConsumer().accept(channel, data);
        onReceive(channel, data);
    }

    /**
     * Logger interface to print messages about broker operations and exceptions.<br>
     * Unlike normal logger implementations, this one uses numbers as levels:<br>
     * 1 = ERROR / SEVERE<br>
     * 2 = WARNING<br>
     * 3 = INFO<br>
     * 4 = DEBUG INFORMATION
     */
    public interface Logger {

        /**
         * Boolean to define if DEBUG INFORMATION will be logged by default.<br>
         * It needs the property {@code saicone.delivery4j.debug} to be set as {@code true}.
         */
        boolean DEBUG = "true".equals(System.getProperty("saicone.delivery4j.debug"));

        /**
         * Create a logger to provided class.<br>
         * This method try to find the best available implementation and uses it.
         *
         * @param clazz the class owning the logger.
         * @return      a newly generated logger instance.
         */
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

        /**
         * Log a message.
         *
         * @param level the message level type.
         * @param msg   the message to log.
         */
        void log(int level, @NotNull String msg);

        /**
         * Log a message, with associated Throwable information.
         *
         * @param level     the message level type.
         * @param msg       the message to log.
         * @param throwable the Throwable associated with log message.
         */
        void log(int level, @NotNull String msg, @NotNull Throwable throwable);

        /**
         * Log a message, which is only to be constructed if the logging level is allowed by current implementation.
         *
         * @param level the message level type.
         * @param msg   the message to log.
         */
        default void log(int level, @NotNull Supplier<String> msg) {
            log(level, msg.get());
        }

        /**
         * Log a message, which is only to be constructed if the logging level is allowed by current implementation.
         *
         * @param level     the message level type.
         * @param msg       the message to log.
         * @param throwable the Throwable associated with log message.
         */
        default void log(int level, @NotNull Supplier<String> msg, @NotNull Throwable throwable) {
            log(level, msg.get(), throwable);
        }
    }
}

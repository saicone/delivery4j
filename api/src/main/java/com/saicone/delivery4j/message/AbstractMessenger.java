package com.saicone.delivery4j.message;

import com.saicone.delivery4j.Broker;
import com.saicone.delivery4j.BrokerListener;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Messenger abstract class to send messages across channels using a {@link Broker}.<br>
 * <br>
 * This class doesn't offer any abstract method making it usable as plain object, but
 * it's suggested to extend it and override {@link AbstractMessenger#loadBroker()} if
 * you want your own implementation.
 *
 * @author Rubenicos
 */
public abstract class AbstractMessenger implements BrokerListener<byte[]>, AutoCloseable {

    private Broker broker;
    private final Map<String, MessageBroker> handlers = new ConcurrentHashMap<>();

    /**
     * Get the current messenger status.
     *
     * @return true if the messenger is enabled
     */
    public boolean isEnabled() {
        return this.broker != null && this.broker.isEnabled();
    }

    /**
     * Get the current broker.
     *
     * @return a broker or null
     */
    @Nullable
    public Broker getBroker() {
        return broker;
    }

    /**
     * Get the current subscribed message brokers, each one with its own channel name.
     *
     * @return a map of channel name and the subscription itself
     */
    @NotNull
    public Map<String, MessageBroker> getHandlers() {
        return handlers;
    }

    /**
     * Set a broker to transfer data.
     *
     * @param broker a broker instance
     */
    public void setBroker(@Nullable Broker broker) {
        this.broker = broker;
    }

    /**
     * Method to load the used broker on data transfer operations.
     *
     * @return a usable broker implementation
     */
    @NotNull
    protected Broker loadBroker() {
        if (getBroker() != null) {
            return getBroker();
        }
        throw new IllegalStateException("Override loadBroker to load a broker or provide one on start messenger instance");
    }

    /**
     * Start the messenger instance connection.
     */
    public void start() {
        start(loadBroker());
    }

    /**
     * Start the messenger instance connection with a provided broker.
     *
     * @param broker the broker to use
     */
    public void start(@NotNull Broker broker) {
        close();

        broker.getSubscribedChannels().addAll(getHandlers().keySet());
        broker.addListener(this);

        this.broker = broker;
        this.broker.start();
    }

    /**
     * Close the messenger instance connection.
     */
    @Override
    public void close() {
        if (this.broker != null) {
            this.broker.close();
        }
    }

    /**
     * Clear any message channels and incoming consumers.
     */
    public void clear() {
        if (this.broker != null) {
            this.broker.clear();
        }
        for (Map.Entry<String, MessageBroker> entry : this.handlers.entrySet()) {
            entry.getValue().clear();
        }
        this.handlers.clear();
    }

    /**
     * Find or create a message broker subscription.<br>
     * This method will make an automatic subscription into broker if it exists
     * and is not subscribed to provided channel name.
     *
     * @param channel the channel name
     * @return        a message channel subscription
     */
    @NotNull
    public MessageBroker subscribe(@NotNull String channel) {
        final MessageBroker messageBroker = this.handlers.computeIfAbsent(channel, MessageBroker::new);
        if (this.broker != null) {
            this.broker.subscribe(channel);
        }
        return messageBroker;
    }

    /**
     * Subscribe to provided message channel.<br>
     * This method will make an automatic subscription into broker if it exists
     * and is not subscribed to provided channel.
     *
     * @param broker the broker to subscribe
     * @return       the previous broker associated with provided channel name, null if there was no mapping for channel name
     */
    @Nullable
    public MessageBroker subscribe(@NotNull MessageBroker broker) {
        if (this.broker != null) {
            this.broker.subscribe(broker.getChannel());
        }
        return this.handlers.put(broker.getChannel(), broker);
    }

    /**
     * Send multi-line message to provided channel name.<br>
     * <br>
     * Take in count this method accept any Object as message lines,
     * but everything will be converted to String.<br>
     * If any object is {@code null} or {@code "null"} it will be sent
     * as null object, and any consumer will get a null object as well.
     *
     * @param channel the channel name to send the message
     * @param lines   the message lines
     * @throws IOException if any I/O error occurs
     * @throws IllegalStateException if the messenger is not enabled or the channel doesn't exist
     */
    public void send(@NotNull String channel, @Nullable Object... lines) throws IOException {
        if (!isEnabled()) {
            throw new IllegalStateException("The messenger is not enabled");
        }

        final MessageBroker messageBroker = this.handlers.get(channel);
        if (messageBroker == null) {
            throw new IllegalStateException("The messaging chanel '" + channel + "' doesn't exist");
        }

        this.broker.send(channel, messageBroker.encode(lines));
    }

    /**
     * Send multi-line message to provided channel name asynchronously.<br>
     * <br>
     * Take in count this method accept any Object as message lines,
     * but everything will be converted to String.<br>
     * If any object is {@code null} or {@code "null"} it will be sent
     * as null object, and any consumer will get a null object as well.
     *
     * @param channel the channel name to send the message
     * @param lines   the message lines
     * @return        a completable future that will complete when the send operation is done
     */
    @NotNull
    public CompletableFuture<Void> sendAsync(@NotNull String channel, @Nullable Object... lines) {
        return CompletableFuture.runAsync(() -> {
            try {
                send(channel, lines);
            } catch (Throwable t) {
                throw new CompletionException(t);
            }
        });
    }

    /**
     * Send multi-line message to provided channel name asynchronously using a provided executor.<br>
     * <br>
     * Take in count this method accept any Object as message lines,
     * but everything will be converted to String.<br>
     * If any object is {@code null} or {@code "null"} it will be sent
     * as null object, and any consumer will get a null object as well.
     *
     * @param executor the executor to run the send operation
     * @param channel  the channel name to send the message
     * @param lines    the message lines
     * @return         a completable future that will complete when the send operation is done
     */
    @NotNull
    public CompletableFuture<Void> sendAsync(@NotNull Executor executor, @NotNull String channel, @Nullable Object... lines) {
        return CompletableFuture.runAsync(() -> {
            try {
                send(channel, lines);
            } catch (Throwable t) {
                throw new CompletionException(t);
            }
        }, executor);
    }

    @Override
    public void accept(@NotNull String channel, byte @NotNull [] src) throws IOException {
        final MessageBroker messageBroker = this.handlers.get(channel);
        if (messageBroker == null) {
            throw new IllegalStateException("The messaging chanel '" + channel + "' doesn't exist");
        }
        messageBroker.accept(src);
    }
}
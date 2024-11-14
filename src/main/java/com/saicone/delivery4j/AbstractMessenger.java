package com.saicone.delivery4j;

import com.saicone.delivery4j.util.ByteCodec;
import com.saicone.delivery4j.util.DelayedExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Messenger abstract class to send messages across channels using a {@link Broker}.<br>
 * This class doesn't offer any abstract method making it usable as plain object, but
 * it's suggested to extend it and override {@link AbstractMessenger#loadBroker()} if
 * you want your own implementation.
 *
 * @author Rubenicos
 */
public abstract class AbstractMessenger {

    private Executor executor = CompletableFuture.completedFuture(null).defaultExecutor();
    private Broker broker;
    private final Map<String, MessageChannel> channels = new HashMap<>();

    /**
     * Get the current messenger status.
     *
     * @return true if the messenger is enabled.
     */
    public boolean isEnabled() {
        return this.broker != null && this.broker.isEnabled();
    }

    /**
     * Get the current executor, by default {@link CompletableFuture#defaultExecutor()} is used.
     *
     * @return a executor used to run any {@link CompletableFuture} used in this class.
     */
    @NotNull
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Get the current broker.
     *
     * @return a broker or null.
     */
    @Nullable
    public Broker getBroker() {
        return broker;
    }

    /**
     * Get the current subscribed channels.
     *
     * @return a map of channel name and the subscription itself.
     */
    @NotNull
    public Map<String, MessageChannel> getChannels() {
        return channels;
    }

    /**
     * Replace the current executor with a custom implementation.
     *
     * @param executor a executor to run any {@link CompletableFuture} operation in this class.
     */
    public void setExecutor(@NotNull Executor executor) {
        this.executor = executor;
    }

    /**
     * Set a broker to transfer data.
     *
     * @param broker a broker instance.
     */
    public void setBroker(@Nullable Broker broker) {
        this.broker = broker;
    }

    /**
     * Method to load the used broker on data transfer operations.
     *
     * @return a usable broker implementation.
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
     * @param broker the broker to use.
     */
    @SuppressWarnings("unchecked")
    public void start(@NotNull Broker broker) {
        close();

        if (this instanceof Executor) {
            this.executor = (Executor) this;
        }

        broker.getSubscribedChannels().addAll(getChannels().keySet());
        broker.setConsumer(this::accept);
        if (this instanceof ByteCodec) {
            try {
                broker.setCodec((ByteCodec<String>) this);
            } catch (Throwable ignored) { }
        }
        if (this instanceof DelayedExecutor) {
            broker.setExecutor((DelayedExecutor<?>) this);
        }

        this.broker = broker;
        this.broker.start();
    }

    /**
     * Close the messenger instance connection.
     */
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
        for (Map.Entry<String, MessageChannel> entry : this.channels.entrySet()) {
            entry.getValue().clear();
        }
        this.channels.clear();
    }

    /**
     * Find or create a message channel subscription.<br>
     * This method will make an automatic subscription into broker if it exists
     * and is not subscribed to provided channel name.
     *
     * @param channel the channel name.
     * @return        a message channel subscription.
     */
    @NotNull
    public MessageChannel subscribe(@NotNull String channel) {
        // note: Do NOT replace with Map#computeIfAbsent()
        MessageChannel messageChannel = this.channels.get(channel);
        if (messageChannel == null) {
            messageChannel = new MessageChannel(channel);
            this.channels.put(channel, messageChannel);
        }
        if (this.broker != null) {
            this.broker.subscribe(channel);
        }
        return messageChannel;
    }

    /**
     * Subscribe to provided message channel.<br>
     * his method will make an automatic subscription into broker if it exists
     * and is not subscribed to provided channel.
     *
     * @param channel the channel to subscribe.
     * @return        the previous channel associated with provided channel name, null if there was no mapping for channel name.
     */
    @Nullable
    public MessageChannel subscribe(@NotNull MessageChannel channel) {
        if (this.broker != null) {
            this.broker.subscribe(channel.getName());
        }
        return this.channels.put(channel.getName(), channel);
    }

    /**
     * Send multi-line message to provided channel name.<br>
     * Take in count this method accept any Object as message lines,
     * but everything will be converted to String.<br>
     * If any object is {@code null} or {@code "null"} it will be sent
     * as null object, and any consumer will get a null object as well.
     *
     * @param channel the channel name to send the message.
     * @param lines   the message lines.
     * @return        a {@link CompletableFuture} executed when this method is called.
     */
    @NotNull
    public CompletableFuture<Void> send(@NotNull String channel, @Nullable Object... lines) {
        if (!isEnabled()) {
            throw new IllegalStateException("The messenger is not enabled");
        }
        final MessageChannel messageChannel = this.channels.get(channel);
        if (messageChannel == null) {
            throw new IllegalStateException("The messaging chanel '" + channel + "' doesn't exist");
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                this.broker.send(channel, messageChannel.encode(lines));
                return null;
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        }, this.executor);
    }

    /**
     * Receive provided byte data to be encoded as readable multi-line message.
     *
     * @param channel the channel name where the data come from.
     * @param src     the byte array to encode as readable message.
     * @return        true if the provided data was accepted by any message channel.
     * @throws IOException if any error occurs in this operation.
     */
    public boolean accept(@NotNull String channel, byte[] src) throws IOException {
        final MessageChannel messageChannel = this.channels.get(channel);
        if (messageChannel == null) {
            throw new IllegalStateException("The messaging chanel '" + channel + "' doesn't exist");
        }
        return messageChannel.accept(src);
    }
}
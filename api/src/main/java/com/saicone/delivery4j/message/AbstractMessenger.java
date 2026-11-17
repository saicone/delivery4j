package com.saicone.delivery4j.message;

import com.saicone.delivery4j.Broker;
import com.saicone.delivery4j.BrokerListener;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Messenger abstract class to send messages across channels using a {@link Broker}.<br>
 * <br>
 * This class doesn't offer any abstract method making it usable as plain object, but
 * it's suggested to extend it and override {@link AbstractMessenger#loadBroker()} if
 * you want your own implementation.
 *
 * @author Rubenicos
 */
public abstract class AbstractMessenger implements Messenger, BrokerListener<byte[]>, AutoCloseable {

    private Broker broker;
    private final Map<String, MessageBroker> handlers = new ConcurrentHashMap<>();

    @Override
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
    @Override
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

    @Override
    public void close() {
        if (this.broker != null) {
            this.broker.close();
        }
    }

    /**
     * Clear any message channels and incoming consumers.
     */
    @Override
    public void clear() {
        if (this.broker != null) {
            this.broker.clear();
        }
        for (Map.Entry<String, MessageBroker> entry : this.handlers.entrySet()) {
            entry.getValue().clear();
        }
        this.handlers.clear();
    }

    @Override
    public @NotNull MessageBroker subscribe(@NotNull String channel) {
        final MessageBroker messageBroker = this.handlers.computeIfAbsent(channel, MessageBroker::new);
        if (this.broker != null) {
            this.broker.subscribe(channel);
        }
        return messageBroker;
    }

    @Override
    public @Nullable MessageBroker subscribe(@NotNull MessageBroker broker) {
        if (this.broker != null) {
            this.broker.subscribe(broker.getChannel());
        }
        return this.handlers.put(broker.getChannel(), broker);
    }

    @Override
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

    @Override
    public void accept(@NotNull String channel, byte @NotNull [] src) throws IOException {
        final MessageBroker messageBroker = this.handlers.get(channel);
        if (messageBroker == null) {
            throw new IllegalStateException("The messaging chanel '" + channel + "' doesn't exist");
        }
        messageBroker.accept(src);
    }
}
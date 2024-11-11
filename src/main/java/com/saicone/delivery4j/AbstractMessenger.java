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
 * Messenger abstract class to send messages across channels using a {@link Broker}.
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

    @NotNull
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Get the current delivery client.
     *
     * @return a delivery client or null.
     */
    @Nullable
    public Broker getBroker() {
        return broker;
    }

    @NotNull
    public Map<String, MessageChannel> getChannels() {
        return channels;
    }

    public void setExecutor(@NotNull Executor executor) {
        this.executor = executor;
    }

    public void setBroker(@Nullable Broker broker) {
        this.broker = broker;
    }

    /**
     * Method to load the used delivery client on data transfer operations.
     *
     * @return an usable delivery client.
     */
    @NotNull
    protected Broker loadBroker() {
        if (getBroker() != null) {
            return getBroker();
        }
        throw new IllegalStateException("Override loadBroker to load a broker or provide one on start messenger instance");
    }

    /**
     * Start the messenger instance.
     */
    public void start() {
        start(loadBroker());
    }

    /**
     * Start the messenger instance with a provided delivery client.
     *
     * @param broker the delivery client to use.
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
     * Stop the messenger instance.
     */
    public void close() {
        if (this.broker != null) {
            this.broker.close();
        }
    }

    /**
     * Clear any delivery client channels and incoming consumers.
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

    @NotNull
    public MessageChannel subscribe(@NotNull String channel) {
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

    @Nullable
    public MessageChannel subscribe(@NotNull MessageChannel channel) {
        return this.channels.put(channel.getName(), channel);
    }

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

    public boolean accept(@NotNull String channel, byte[] src) throws IOException {
        final MessageChannel messageChannel = this.channels.get(channel);
        if (messageChannel == null) {
            throw new IllegalStateException("The messaging chanel '" + channel + "' doesn't exist");
        }
        return messageChannel.accept(src);
    }
}
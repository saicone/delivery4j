package com.saicone.delivery4j.message;

import com.saicone.delivery4j.Broker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Messenger interface to send messages across channels using a {@link Broker}.<br>
 * <br>
 * This interface is intended to be implemented by classes that want to provide
 * messaging capabilities using a broker, allowing for sending messages to specific channels
 * and managing subscriptions to those channels.
 *
 * @author Rubenicos
 */
public interface Messenger extends AutoCloseable {

    /**
     * Encapsulate a broker into a simple messenger instance.<br>
     * Take in count that closing the messenger instance will
     * also close the broker instance.
     *
     * @param broker a broker instance
     * @return       a newly generated messenger instance
     */
    @NotNull
    static Messenger simple(@NotNull Broker broker) {
        final AbstractMessenger messenger = new AbstractMessenger() { };
        messenger.setBroker(broker);
        return messenger;
    }

    /**
     * Create a simple messenger instance using a broker constructor.
     *
     * @param constructor a supplier that provides a broker instance
     * @return            a newly generated messenger instance
     */
    @NotNull
    static Messenger simple(@NotNull Supplier<Broker> constructor) {
        return new AbstractMessenger() {
            @Override
            protected @NotNull Broker loadBroker() {
                return constructor.get();
            }
        };
    }

    /**
     * Get the current messenger status.
     *
     * @return true if the messenger is enabled
     */
    boolean isEnabled();

    /**
     * Start the messenger instance connection.
     */
    void start();

    /**
     * Close the messenger instance connection.
     */
    @Override
    void close();

    /**
     * Close the messenger instance.
     */
    void clear();

    /**
     * Find or create a message broker subscription.<br>
     * This method is intended to make an automatic subscription into broker
     * if it exists and is not subscribed to provided channel name.
     *
     * @param channel the channel name
     * @return        a message channel subscription
     */
    @NotNull
    MessageBroker subscribe(@NotNull String channel);

    /**
     * Subscribe to provided message channel.<br>
     * This method is intended make an automatic subscription into broker
     * if it exists and is not subscribed to provided channel.
     *
     * @param broker the broker to subscribe
     * @return       the previous broker associated with provided channel name, null if there was no mapping for channel name
     */
    @Nullable
    MessageBroker subscribe(@NotNull MessageBroker broker);

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
    void send(@NotNull String channel, @Nullable Object... lines) throws IOException;

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
    default CompletableFuture<Void> sendAsync(@NotNull String channel, @Nullable Object... lines) {
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
    default CompletableFuture<Void> sendAsync(@NotNull Executor executor, @NotNull String channel, @Nullable Object... lines) {
        return CompletableFuture.runAsync(() -> {
            try {
                send(channel, lines);
            } catch (Throwable t) {
                throw new CompletionException(t);
            }
        }, executor);
    }
}

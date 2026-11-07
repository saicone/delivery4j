package com.saicone.delivery4j;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * Represents and operation that accept a channel name and the data produced by the channel.<br>
 * Unlike {@link java.util.function.Consumer}, this operation can throw an {@link IOException}.
 *
 * @author Rubenicos
 *
 * @param <T> the type of data produced by the channel.
 */
@FunctionalInterface
public interface ChannelConsumer<T> {

    /**
     * Performs this operation with the given channel name and the data produced by itself.
     *
     * @param channel the channel name.
     * @param src     the data to be processed.
     * @throws IOException if any error occurs during this operation.
     */
    void accept(@NotNull String channel, @NotNull T src) throws IOException;

    /**
     * Returns a composed {@link ChannelConsumer} that performs, in sequence, this operation followed by the {@code after} operation.
     *
     * @param after the operation to perform after this operation.
     * @return      a composed {@link ChannelConsumer} that performs, in sequence, this operation followed by the {@code after} operation.
     */
    default ChannelConsumer<T> andThen(@NotNull ChannelConsumer<T> after) {
        return (channel, src) -> {
            accept(channel, src);
            after.accept(channel, src);
        };
    }
}

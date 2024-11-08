package com.saicone.delivery4j;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;

@FunctionalInterface
public interface ChannelConsumer<T> {

    void accept(@NotNull String channel, @NotNull T src) throws IOException;

    default ChannelConsumer<T> andThen(@NotNull ChannelConsumer<T> after) {
        return (channel, src) -> {
            accept(channel, src);
            after.accept(channel, src);
        };
    }
}

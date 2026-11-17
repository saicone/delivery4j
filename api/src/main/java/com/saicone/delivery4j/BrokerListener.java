package com.saicone.delivery4j;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Comparator;

/**
 * Represents and operation that accept a channel name and the data produced by a {@link Broker}.<br>
 * <br>
 * Unlike {@link java.util.function.Consumer}, this operation can throw an {@link IOException}.
 *
 * @author Rubenicos
 *
 * @param <T> the type of data produced by the channel
 */
@FunctionalInterface
public interface BrokerListener<T> extends Comparable<BrokerListener<T>> {

    /**
     * The default priority of a {@link BrokerListener}, using natural order.<br>
     * Meaning that the lower the number, the higher the priority.
     */
    int DEFAULT_PRIORITY = 100;

    /**
     * A simple comparator that compares {@link BrokerListener} by their priority
     * and then by their identity hash code to avoid collisions.
     */
    Comparator<BrokerListener<?>> COMPARATOR = Comparator.<BrokerListener<?>>comparingInt(BrokerListener::priority)
                    .thenComparingInt(System::identityHashCode);

    /**
     * Get the priority of this listener.
     *
     * @return the priority of this listener
     */
    default int priority() {
        return DEFAULT_PRIORITY;
    }

    /**
     * Performs this operation with the given channel name and the data produced by a {@link Broker}.
     *
     * @param channel the channel name
     * @param src     the data to be processed
     * @throws IOException if any error occurs during this operation
     */
    void accept(@NotNull String channel, @NotNull T src) throws IOException;

    /**
     * Returns a composed {@link BrokerListener} that performs, in sequence, this operation followed by the {@code after} operation.
     *
     * @param after the operation to perform after this operation
     * @return      a composed {@link BrokerListener}
     */
    default BrokerListener<T> andThen(@NotNull BrokerListener<T> after) {
        return (channel, src) -> {
            accept(channel, src);
            after.accept(channel, src);
        };
    }

    @Override
    default int compareTo(@NotNull BrokerListener<T> o) {
        return Integer.compare(this.priority(), o.priority());
    }
}

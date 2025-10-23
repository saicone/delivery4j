package com.saicone.delivery4j;

import com.saicone.delivery4j.util.Encryptor;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * An object to consume channel messages.<br>
 * This object can also provide an {@link Encryptor} to make a secure message delivery.
 *
 * @author Rubenicos
 */
public class MessageChannel {

    private final String name;
    private ChannelConsumer<String[]> consumer;
    private Cache cache;
    private Encryptor encryptor = Encryptor.empty();

    /**
     * Create a message channel with provided name.
     *
     * @param name the channel name.
     * @return     a newly generated message channel.
     */
    @NotNull
    public static MessageChannel of(@NotNull String name) {
        return new MessageChannel(name);
    }

    /**
     * Constructs a message channel with provided name.
     *
     * @param name the channel name.
     */
    public MessageChannel(@NotNull String name) {
        this.name = name;
    }

    /**
     * Constructs a message channel with provided name and consumer.
     *
     * @param name     the channel name.
     * @param consumer the consumer that accept multi-line messages.
     */
    public MessageChannel(@NotNull String name, @Nullable ChannelConsumer<String[]> consumer) {
        this.name = name;
        this.consumer = consumer;
    }

    /**
     * Get the current channel name.
     *
     * @return a channel name.
     */
    @NotNull
    public String getName() {
        return name;
    }

    /**
     * Get the current message consumer.
     *
     * @return a channel consumer that accept multi-line messages.
     */
    @Nullable
    public ChannelConsumer<String[]> getConsumer() {
        return consumer;
    }

    /**
     * Get the current cache instance.
     *
     * @return a message ID cache if exists, null otherwise.
     */
    @Nullable
    public Cache getCache() {
        return cache;
    }

    /**
     * Get the current encryptor.
     *
     * @return a message encryptor if exists, null otherwise.
     */
    @Nullable
    public Encryptor getEncryptor() {
        return encryptor;
    }

    /**
     * Set or append provided consumer into channel inbound consumer.
     *
     * @param consumer the consumer that accept multi-line messages.
     * @return         the current message channel.
     */
    @NotNull
    @Contract("_ -> this")
    public MessageChannel consume(@NotNull ChannelConsumer<String[]> consumer) {
        if (this.consumer == null) {
            this.consumer = consumer;
        } else {
            this.consumer = this.consumer.andThen(consumer);
        }
        return this;
    }

    /**
     * Set or append before provided consumer into channel inbound consumer.
     *
     * @param consumer the consumer that accept multi-line messages.
     * @return         the current message channel.
     */
    @NotNull
    @Contract("_ -> this")
    public MessageChannel consumeBefore(@NotNull ChannelConsumer<String[]> consumer) {
        if (this.consumer == null) {
            this.consumer = consumer;
        } else {
            this.consumer = consumer.andThen(this.consumer);
        }
        return this;
    }

    /**
     * Set caching status for the current message channel.
     *
     * @param enable true to use default cache parameters, false to remove any cache instance.
     * @return       the current message channel.
     */
    @NotNull
    @Contract("_ -> this")
    public MessageChannel cache(boolean enable) {
        if (enable) {
            return cache(10, TimeUnit.SECONDS);
        } else {
            return cache(null);
        }
    }

    /**
     * Set the cache time for the current message channel.
     *
     * @param duration the time to wait until any message ID will be deleted.
     * @param unit     the unit for the provided time.
     * @return         the current message channel.
     */
    @NotNull
    @Contract("_, _ -> this")
    public MessageChannel cache(long duration, @NotNull TimeUnit unit) {
        return cache(Cache.of(duration, unit));
    }

    /**
     * Set the cache instance for the current message channel.
     *
     * @param cache the message ID cache.
     * @return      the current message channel.
     */
    @NotNull
    @Contract("_ -> this")
    public MessageChannel cache(@Nullable Cache cache) {
        this.cache = cache;
        return this;
    }

    /**
     * Set the message encryptor for the current message channel.
     *
     * @param encryptor the message encryptor.
     * @return          the current message channel.
     */
    @NotNull
    @Contract("_ -> this")
    public MessageChannel encryptor(@NotNull Encryptor encryptor) {
        this.encryptor = encryptor;
        return this;
    }

    /**
     * Encodes the specified message lines into byte array.
     *
     * @param lines message to encode.
     * @return      a byte array that represent the message.
     * @throws IOException if the message lines cannot be encoded as bytes.
     */
    public byte[] encode(@Nullable Object... lines) throws IOException {
        try (ByteArrayOutputStream arrayOut = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(arrayOut)) {
            if (this.cache != null) {
                out.writeInt(this.cache.generate());
            }
            out.writeInt(lines.length);
            for (Object message : lines) {
                this.encryptor.writeUTF(out, Objects.toString(message));
            }
            return arrayOut.toByteArray();
        }
    }

    /**
     * Decodes a byte array into a multi-line message.
     *
     * @param src the byte array to decode.
     * @return    a message from byte array.
     * @throws IOException if the bytes cannot be decoded from bytes.
     */
    @Nullable
    public String[] decode(byte[] src) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(src))) {
            if (this.cache != null && this.cache.contains(in.readInt())) {
                return null;
            }
            final String[] lines = new String[in.readInt()];
            try {
                for (int i = 0; i < lines.length; i++) {
                    final String message = this.encryptor.readUTF(in);
                    lines[i] = message.equalsIgnoreCase("null") ? null : message;
                }
            } catch (EOFException ignored) { }
            return lines;
        }
    }

    /**
     * Accept the provided pre-decoded data into current consumer.
     *
     * @param src the byte array to decode.
     * @return    true if the data was processed correctly, false otherwise.
     * @throws IOException if any error occurs in this operation.
     */
    public boolean accept(byte[] src) throws IOException {
        final String[] lines = decode(src);
        if (lines == null) {
            return false;
        }
        if (this.consumer != null) {
            this.consumer.accept(getName(), lines);
        }
        return true;
    }

    /**
     * Clear the current message channel instance.
     */
    public void clear() {
        if (this.cache != null) {
            this.cache.clear();
        }
    }

    /**
     * Cache interface to store message IDs with certain delay.
     */
    public static abstract class Cache {

        /**
         * Create a cache with provided expiration.<br>
         * This method try to find the best available implementation and uses it.
         *
         * @param duration the length of time after a message ID is automatically removed.
         * @param unit     the unit that {@code duration} is expressed in.
         * @return         a newly generate cache instance.
         */
        @NotNull
        public static Cache of(long duration, @NotNull TimeUnit unit) {
            try {
                Class.forName("com.github.benmanes.caffeine.cache.Caffeine");
                return Class.forName("com.saicone.delivery4j.cache.CaffeineCache")
                        .asSubclass(Cache.class)
                        .getDeclaredConstructor(long.class, TimeUnit.class)
                        .newInstance(duration, unit);
            } catch (Throwable ignored) { }

            try {
                Class.forName("com.google.common.cache.CacheBuilder");
                return Class.forName("com.saicone.delivery4j.cache.GuavaCache")
                        .asSubclass(Cache.class)
                        .getDeclaredConstructor(long.class, TimeUnit.class)
                        .newInstance(duration, unit);
            } catch (Throwable ignored) { }

            return new Cache() {
                private final Map<Integer, Long> cache = new HashMap<>();
                private final long millis = unit.toMillis(duration);

                @Override
                protected void save(int id) {
                    final long currentTime = System.currentTimeMillis();
                    if (id < 1999) { // 20%
                        final long time = currentTime - this.millis;
                        this.cache.entrySet().removeIf(entry -> entry.getValue() <= time);
                    }
                    this.cache.put(id, currentTime);
                }

                @Override
                public boolean contains(int id) {
                    return this.cache.containsKey(id);
                }

                @Override
                public void clear() {
                    this.cache.clear();
                }
            };
        }

        /**
         * Save message ID.
         *
         * @param id the ID of the message to save.
         */
        protected abstract void save(int id);

        /**
         * Check if the current cache contains the provided message ID.
         *
         * @param id the ID of the message.
         * @return   true if the message is already saved, false otherwise.
         */
        public abstract boolean contains(int id);

        /**
         * Generate message ID and save into cache.
         *
         * @return a message ID.
         */
        public int generate() {
            final int id = ThreadLocalRandom.current().nextInt(0, 999999 + 1);
            save(id);
            return id;
        }

        /**
         * Clear the current cache instance values.
         */
        public abstract void clear();
    }
}

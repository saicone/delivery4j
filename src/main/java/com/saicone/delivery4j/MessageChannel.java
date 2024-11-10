package com.saicone.delivery4j;

import com.saicone.delivery4j.util.Encryptor;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
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

public class MessageChannel {

    private final String name;
    private ChannelConsumer<String[]> consumer;
    private Cache cache;
    private Encryptor encryptor;

    public MessageChannel(@NotNull String name) {
        this.name = name;
    }

    public MessageChannel(@NotNull String name, @Nullable ChannelConsumer<String[]> consumer) {
        this.name = name;
        this.consumer = consumer;
    }

    @NotNull
    public String getName() {
        return name;
    }

    @Nullable
    public ChannelConsumer<String[]> getConsumer() {
        return consumer;
    }

    @Nullable
    public Cache getCache() {
        return cache;
    }

    @Nullable
    public Encryptor getEncryptor() {
        return encryptor;
    }

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

    @NotNull
    @Contract("_ -> this")
    public MessageChannel cache(boolean enable) {
        if (enable) {
            return cache(10, TimeUnit.SECONDS);
        } else {
            return cache(null);
        }
    }

    @NotNull
    @Contract("_, _ -> this")
    public MessageChannel cache(long duration, @NotNull TimeUnit unit) {
        return cache(Cache.of(duration, unit));
    }

    @NotNull
    @Contract("_ -> this")
    public MessageChannel cache(@Nullable Cache cache) {
        this.cache = cache;
        return this;
    }

    @NotNull
    @Contract("_ -> this")
    public MessageChannel encryptor(@Nullable Encryptor encryptor) {
        this.encryptor = encryptor;
        return this;
    }

    public byte[] encode(@Nullable Object... lines) throws IOException {
        try (ByteArrayOutputStream arrayOut = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(arrayOut)) {
            if (this.cache != null) {
                out.writeInt(this.cache.generate());
            }
            out.writeInt(lines.length);
            if (this.encryptor == null) {
                for (Object message : lines) {
                    out.writeUTF(Objects.toString(message));
                }
            } else {
                try {
                    for (Object message : lines) {
                        final byte[] bytes = this.encryptor.encrypt(Objects.toString(message));
                        out.writeInt(bytes.length);
                        out.write(bytes);
                    }
                } catch (IllegalBlockSizeException | BadPaddingException e) {
                    throw new IOException("Cannot encrypt message into channel " + this.name, e);
                }
            }
            return arrayOut.toByteArray();
        }
    }

    @Nullable
    public String[] decode(byte[] src) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(src))) {
            if (this.cache != null && this.cache.contains(in.readInt())) {
                return null;
            }
            final String[] lines = new String[in.readInt()];
            try {
                if (this.encryptor == null) {
                    for (int i = 0; i < lines.length; i++) {
                        final String message = in.readUTF();
                        lines[i] = message.equalsIgnoreCase("null") ? null : message;
                    }
                } else {
                    try {
                        for (int i = 0; i < lines.length; i++) {
                            final String message = this.encryptor.decrypt(in.readNBytes(in.readInt()));
                            lines[i] = message.equalsIgnoreCase("null") ? null : message;
                        }
                    } catch (IllegalBlockSizeException | BadPaddingException e) {
                        throw new IOException("Cannot decrypt message from channel " + this.name, e);
                    }
                }
            } catch (EOFException ignored) { }
            return lines;
        }
    }

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

    public void clear() {
        this.cache.clear();
    }

    public static abstract class Cache {

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

        protected abstract void save(int id);

        public abstract boolean contains(int id);

        public int generate() {
            final int id = ThreadLocalRandom.current().nextInt(0, 999999 + 1);
            save(id);
            return id;
        }

        public abstract void clear();
    }
}

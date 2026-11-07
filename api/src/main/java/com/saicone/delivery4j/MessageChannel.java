package com.saicone.delivery4j;

import com.saicone.delivery4j.util.DataIdentifier;
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
import java.util.Objects;

/**
 * An object to consume channel messages.<br>
 * This object can also provide an {@link Encryptor} to make a secure message delivery.
 *
 * @author Rubenicos
 */
public class MessageChannel {

    private final String name;
    private ChannelConsumer<String[]> consumer;
    private DataIdentifier identifier;
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
     * Get the current data identifier.
     *
     * @return a data identifier if exists, null otherwise.
     */
    @Nullable
    public DataIdentifier getIdentifier() {
        return identifier;
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
     * Set the data identifier for the current message channel.
     *
     * @param identifier the data identifier.
     * @return           the current message channel.
     */
    @NotNull
    @Contract("_ -> this")
    public MessageChannel identifier(@Nullable DataIdentifier identifier) {
        this.identifier = identifier;
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
            if (this.identifier != null) {
                this.identifier.write(out);
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
            if (this.identifier != null && !this.identifier.read(in)) {
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
        // empty default method
    }
}

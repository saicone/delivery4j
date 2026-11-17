package com.saicone.delivery4j.message;

import com.saicone.delivery4j.BrokerListener;
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
import java.util.TreeSet;

/**
 * An object that represents the holder of a messaging channel, meaning that is the responsible
 * to encode/decode message data sent through a {@link com.saicone.delivery4j.Broker} and deliver it to the registered listeners.<br>
 * <br>
 * This object can also provide an {@link Encryptor} to make a secure message delivery and
 * a {@link DataIdentifier} to identify the message data and avoid processing self-produced messages.
 *
 * @author Rubenicos
 */
public class MessageBroker {

    private final String channel;
    private final TreeSet<BrokerListener<String[]>> listeners;

    private DataIdentifier identifier;
    private Encryptor encryptor = Encryptor.empty();

    /**
     * Create a message broker with provided registered channel.
     *
     * @param name the channel name
     * @return     a newly generated message channel
     */
    @NotNull
    public static MessageBroker of(@NotNull String name) {
        return new MessageBroker(name);
    }

    /**
     * Constructs a message broker with provided channel.
     *
     * @param channel the channel name
     */
    public MessageBroker(@NotNull String channel) {
        this(channel, new TreeSet<>());
    }

    /**
     * Constructs a message broker with provided channel and listeners.
     *
     * @param channel   the channel name
     * @param listeners the listeners that accept multi-line messages
     */
    public MessageBroker(@NotNull String channel, @NotNull TreeSet<BrokerListener<String[]>> listeners) {
        this.channel = channel;
        this.listeners = listeners;
    }

    /**
     * Get the current channel name.
     *
     * @return a channel name
     */
    @NotNull
    public String getChannel() {
        return channel;
    }

    /**
     * Get the current listeners.
     *
     * @return a set of listeners
     */
    public TreeSet<BrokerListener<String[]>> getListeners() {
        return listeners;
    }

    /**
     * Get the current data identifier.
     *
     * @return a data identifier if exists, null otherwise
     */
    @Nullable
    public DataIdentifier getIdentifier() {
        return identifier;
    }

    /**
     * Get the current encryptor.
     *
     * @return a message encryptor if exists, null otherwise
     */
    @Nullable
    public Encryptor getEncryptor() {
        return encryptor;
    }

    /**
     * Register a new listener to accept multi-line messages.
     *
     * @param consumer the listener to register
     * @return         the current message broker
     */
    @NotNull
    @Contract("_ -> this")
    public MessageBroker listener(@NotNull BrokerListener<String[]> consumer) {
        this.listeners.add(consumer);
        return this;
    }

    /**
     * Set the data identifier for the current message broker.
     *
     * @param identifier the data identifier
     * @return           the current message broker
     */
    @NotNull
    @Contract("_ -> this")
    public MessageBroker identifier(@Nullable DataIdentifier identifier) {
        this.identifier = identifier;
        return this;
    }

    /**
     * Set the message encryptor for the current message broker.
     *
     * @param encryptor the message encryptor
     * @return          the current message broker
     */
    @NotNull
    @Contract("_ -> this")
    public MessageBroker encryptor(@NotNull Encryptor encryptor) {
        this.encryptor = encryptor;
        return this;
    }

    /**
     * Encodes the specified message lines into byte array.
     *
     * @param lines message to encode
     * @return      a byte array that represent the message
     * @throws IOException if the message lines cannot be encoded as bytes
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
     * @param src the byte array to decode
     * @return    a message from byte array
     * @throws IOException if the bytes cannot be decoded from bytes
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
     * @param src the byte array to decode
     * @return    true if the data was processed correctly, false otherwise
     * @throws IOException if any error occurs in this operation
     */
    public boolean accept(byte[] src) throws IOException {
        final String[] lines = decode(src);
        if (lines == null) {
            return false;
        }
        for (BrokerListener<String[]> listener : this.listeners) {
            listener.accept(getChannel(), lines);
        }
        return true;
    }

    /**
     * Clear the current message broker instance.
     */
    public void clear() {
        // empty default method
    }
}

package com.saicone.delivery4j;

import com.saicone.delivery4j.util.ByteCodec;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * Represents a Broker implementation that send and receive plain-text data
 * using a specified byte codec to convert byte arrays into/from String.
 *
 * @author Rubenicos
 */
public abstract class PlainTextBroker extends Broker {

    private ByteCodec<String> codec = ByteCodec.BASE64;

    /**
     * Get the current byte codec.
     *
     * @return a byte codec that convert bytes into/from String.
     */
    @NotNull
    public ByteCodec<String> getCodec() {
        return codec;
    }

    /**
     * Replace the current byte codec.
     *
     * @param codec the byte codec to set.
     */
    public void setCodec(@NotNull ByteCodec<String> codec) {
        this.codec = codec;
    }

    @Override
    public void send(@NotNull String channel, byte[] data) throws IOException {
        send(channel, getCodec().encode(data));
    }

    /**
     * Send plain-text data to specified channel.
     *
     * @param channel the channel name.
     * @param data    the plain-text data to send.
     * @throws IOException if an I/O error occurs.
     */
    public abstract void send(@NotNull String channel, @NotNull String data) throws IOException;

    /**
     * Receive plain-text data from specified channel.
     *
     * @param channel the channel name.
     * @param data    the plain-text data received.
     * @throws IOException if an I/O error occurs.
     */
    public void receive(@NotNull String channel, @NotNull String data) throws IOException {
        receive(channel, getCodec().decode(data));
    }
}

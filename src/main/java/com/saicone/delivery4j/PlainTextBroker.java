package com.saicone.delivery4j;

import com.saicone.delivery4j.util.ByteCodec;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

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

    public abstract void send(@NotNull String channel, @NotNull String data) throws IOException;

    public void receive(@NotNull String channel, @NotNull String data) throws IOException {
        receive(channel, getCodec().decode(data));
    }
}

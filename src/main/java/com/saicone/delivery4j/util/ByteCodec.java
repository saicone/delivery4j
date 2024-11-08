package com.saicone.delivery4j.util;

import org.jetbrains.annotations.NotNull;

import java.util.Base64;

public interface ByteCodec<T> {

    ByteCodec<String> BASE64 = new ByteCodec<>() {
        @Override
        public @NotNull String encode(byte[] src) {
            return Base64.getEncoder().encodeToString(src);
        }

        @Override
        public byte[] decode(@NotNull String src) {
            return Base64.getDecoder().decode(src);
        }
    };

    @NotNull
    T encode(byte[] src);

    byte[] decode(@NotNull T src);
}

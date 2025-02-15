package com.saicone.delivery4j.util;

import org.jetbrains.annotations.NotNull;

import java.util.Base64;

/**
 * Byte codec interface to encode and decode bytes into desired object.
 *
 * @author Rubenicos
 *
 * @param <T> the object type to convert bytes into/from.
 */
public interface ByteCodec<T> {

    /**
     * Base64 byte codec that convert bytes into/from Base64 String.
     */
    ByteCodec<String> BASE64 = new ByteCodec<String>() {
        @Override
        public @NotNull String encode(byte[] src) {
            return Base64.getEncoder().encodeToString(src);
        }

        @Override
        public byte[] decode(@NotNull String src) {
            return Base64.getDecoder().decode(src);
        }
    };

    /**
     * Encodes the specified byte array into object type.
     *
     * @param src the byte array to encode.
     * @return    an object type that represent the bytes.
     */
    @NotNull
    T encode(byte[] src);

    /**
     * Decodes an object type into a newly-allocated byte array.
     *
     * @param src the objet type to decode.
     * @return    a byte array from object type.
     */
    byte[] decode(@NotNull T src);
}

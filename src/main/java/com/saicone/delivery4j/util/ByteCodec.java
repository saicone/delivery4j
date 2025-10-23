package com.saicone.delivery4j.util;

import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;
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
     * Z85 byte codec that convert bytes into/from Z85 String.
     */
    ByteCodec<String> Z85 = new ByteCodec<String>() {
        @Override
        public @NotNull String encode(byte[] src) {
            return com.saicone.delivery4j.util.Z85.getEncoder().encode(src);
        }

        @Override
        public byte[] decode(@NotNull String src) {
            return com.saicone.delivery4j.util.Z85.getDecoder().decode(src);
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
     * Encodes the specified String into object type using default charset.
     *
     * @param src the String to encode.
     * @return    an object type that represent the String bytes.
     */
    @NotNull
    default T encodeFromString(@NotNull String src) {
        return encodeFromString(src, Charset.defaultCharset());
    }

    /**
     * Encodes the specified String into object type using the specified charset.
     *
     * @param src     the String to encode.
     * @param charset the charset to be used to get bytes from String.
     * @return        an object type that represent the String bytes.
     */
    @NotNull
    default T encodeFromString(@NotNull String src, @NotNull Charset charset) {
        return encode(src.getBytes(charset));
    }

    /**
     * Decodes an object type into a newly-allocated byte array.
     *
     * @param src the objet type to decode.
     * @return    a byte array from object type.
     */
    byte[] decode(@NotNull T src);

    /**
     * Decodes an object type into a String using default charset.
     *
     * @param src the objet type to decode.
     * @return    a String from object type.
     */
    @NotNull
    default String decodeToString(@NotNull T src) {
        return decodeToString(src, Charset.defaultCharset());
    }

    /**
     * Decodes an object type into a String using the specified charset.
     *
     * @param src     the objet type to decode.
     * @param charset the charset to be used to decode data into String.
     * @return        a String from object type.
     */
    @NotNull
    default String decodeToString(@NotNull T src, @NotNull Charset charset) {
        return new String(decode(src), charset);
    }
}

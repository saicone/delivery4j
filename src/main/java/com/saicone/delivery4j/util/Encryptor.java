package com.saicone.delivery4j.util;

import org.jetbrains.annotations.NotNull;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public interface Encryptor {

    @NotNull
    static Encryptor of(@NotNull SecretKey key) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        return of("AES", key, StandardCharsets.UTF_8);
    }

    @NotNull
    static Encryptor of(@NotNull SecretKey key, @NotNull Charset charset) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        return of("AES", key, charset);
    }

    @NotNull
    static Encryptor of(@NotNull String transformation, @NotNull SecretKey key) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        return of(transformation, key, StandardCharsets.UTF_8);
    }

    @NotNull
    static Encryptor of(@NotNull String transformation, @NotNull SecretKey key, @NotNull Charset charset) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        final Cipher encrypt = Cipher.getInstance(transformation);
        encrypt.init(Cipher.ENCRYPT_MODE, key);
        final Cipher decrypt = Cipher.getInstance(transformation);
        decrypt.init(Cipher.DECRYPT_MODE, key);
        return new Encryptor() {
            @Override
            public byte[] encrypt(@NotNull String input) throws IllegalBlockSizeException, BadPaddingException {
                return encrypt.doFinal(input.getBytes(charset));
            }

            @Override
            public @NotNull String decrypt(byte[] input) throws IllegalBlockSizeException, BadPaddingException {
                return new String(decrypt.doFinal(input), charset);
            }
        };
    }

    byte[] encrypt(@NotNull String input) throws IllegalBlockSizeException, BadPaddingException;

    @NotNull
    String decrypt(byte[] input) throws IllegalBlockSizeException, BadPaddingException;
}

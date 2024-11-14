package com.saicone.delivery4j.util;

import org.jetbrains.annotations.NotNull;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * Encryptor interface to encrypt and decrypt any provided String.<br>
 * By default, this is just a bridge to regular Java Cipher usage.
 *
 * @author Rubenicos
 */
public interface Encryptor {

    /**
     * Create an encryptor with provided arguments that performs automatic resets if any error occurs on encryption/decryption.
     *
     * @param key            the key.
     * @return               an encryptor instance.
     * @throws NoSuchPaddingException if {@code transformation} contains a padding scheme that is not available.
     * @throws NoSuchAlgorithmException if {@code transformation} is {@code null}, empty, in an invalid format, or if no {@code Provider} supports a {@code CipherSpi} implementation for the specified algorithm.
     * @throws InvalidKeyException if the given key is inappropriate for cipher instances.
     */
    @NotNull
    static Encryptor of(@NotNull SecretKey key) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        return of("AES", key, StandardCharsets.UTF_8);
    }

    /**
     * Create an encryptor with provided arguments that performs automatic resets if any error occurs on encryption/decryption.
     *
     * @param key            the key.
     * @param charset        the charset to be used to decode data into String.
     * @return               an encryptor instance.
     * @throws NoSuchPaddingException if {@code transformation} contains a padding scheme that is not available.
     * @throws NoSuchAlgorithmException if {@code transformation} is {@code null}, empty, in an invalid format, or if no {@code Provider} supports a {@code CipherSpi} implementation for the specified algorithm.
     * @throws InvalidKeyException if the given key is inappropriate for cipher instances.
     */
    @NotNull
    static Encryptor of(@NotNull SecretKey key, @NotNull Charset charset) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        return of("AES", key, charset);
    }

    /**
     * Create an encryptor with provided arguments that performs automatic resets if any error occurs on encryption/decryption.
     *
     * @param transformation the name of the transformation.
     * @param key            the key.
     * @return               an encryptor instance.
     * @throws NoSuchPaddingException if {@code transformation} contains a padding scheme that is not available.
     * @throws NoSuchAlgorithmException if {@code transformation} is {@code null}, empty, in an invalid format, or if no {@code Provider} supports a {@code CipherSpi} implementation for the specified algorithm.
     * @throws InvalidKeyException if the given key is inappropriate for cipher instances.
     */
    @NotNull
    static Encryptor of(@NotNull String transformation, @NotNull SecretKey key) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        return of(transformation, key, StandardCharsets.UTF_8);
    }

    /**
     * Create an encryptor with provided arguments that performs automatic resets if any error occurs on encryption/decryption.
     *
     * @param transformation the name of the transformation.
     * @param key            the key.
     * @param charset        the charset to be used to decode data into String.
     * @return               an encryptor instance.
     * @throws NoSuchPaddingException if {@code transformation} contains a padding scheme that is not available.
     * @throws NoSuchAlgorithmException if {@code transformation} is {@code null}, empty, in an invalid format, or if no {@code Provider} supports a {@code CipherSpi} implementation for the specified algorithm.
     * @throws InvalidKeyException if the given key is inappropriate for cipher instances.
     */
    @NotNull
    static Encryptor of(@NotNull String transformation, @NotNull SecretKey key, @NotNull Charset charset) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        final Cipher encryptMode = Cipher.getInstance(transformation);
        encryptMode.init(Cipher.ENCRYPT_MODE, key);
        final Cipher decryptMode = Cipher.getInstance(transformation);
        decryptMode.init(Cipher.DECRYPT_MODE, key);
        return new Encryptor() {
            private Cipher encrypt = encryptMode;
            private Cipher decrypt = decryptMode;

            @Override
            public byte[] encrypt(@NotNull String input) {
                try {
                    return encrypt.doFinal(input.getBytes(charset));
                } catch (Throwable t) {
                    try {
                        encrypt = Cipher.getInstance(transformation);
                        encrypt.init(Cipher.ENCRYPT_MODE, key);
                    } catch (Exception ignored) { }
                    throw new RuntimeException(t);
                }
            }

            @Override
            public @NotNull String decrypt(byte[] input) {
                try {
                    return new String(decrypt.doFinal(input), charset);
                } catch (Throwable t) {
                    try {
                        decrypt = Cipher.getInstance(transformation);
                        decrypt.init(Cipher.DECRYPT_MODE, key);
                    } catch (Exception ignored) { }
                    throw new RuntimeException(t);
                }
            }
        };
    }

    /**
     * Encrypts the input String data and return itself as byte array.
     *
     * @param input the String to encrypt.
     * @return      an encrypted String data.
     */
    byte[] encrypt(@NotNull String input);

    /**
     * Decrypts the input data and return itself as readable String.
     *
     * @param input the byte array to decrypt.
     * @return      a decrypted String.
     */
    @NotNull
    String decrypt(byte[] input);
}

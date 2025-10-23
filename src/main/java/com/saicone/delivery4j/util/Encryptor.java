package com.saicone.delivery4j.util;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
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
     * Empty encryptor instance.
     */
    @ApiStatus.Internal
    Encryptor EMPTY = new Encryptor() {
        @Override
        public byte[] encrypt(byte[] input) {
            return input;
        }

        @Override
        public byte[] decrypt(byte[] input) {
            return input;
        }

        @Override
        public void writeUTF(@NotNull DataOutput out, @NotNull String input) throws IOException {
            out.writeUTF(input);
        }

        @Override
        public @NotNull String readUTF(@NotNull DataInput in) throws IOException {
            return in.readUTF();
        }
    };

    /**
     * Get an empty encryptor that does not perform any encryption/decryption.<br>
     * This instance also writes and reads data using DataOutput/DataInput UTF methods.
     *
     * @return an empty encryptor instance.
     */
    @NotNull
    static Encryptor empty() {
        return EMPTY;
    }

    /**
     * Create an encryptor with provided arguments that performs automatic resets if any error occurs on encryption/decryption.
     *
     * @param key the key.
     * @return    an encryptor instance.
     * @throws NoSuchPaddingException if {@code transformation} contains a padding scheme that is not available.
     * @throws NoSuchAlgorithmException if {@code transformation} is {@code null}, empty, in an invalid format, or if no {@code Provider} supports a {@code CipherSpi} implementation for the specified algorithm.
     * @throws InvalidKeyException if the given key is inappropriate for cipher instances.
     */
    @NotNull
    static Encryptor of(@NotNull SecretKey key) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        return of("AES", key);
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
        final Cipher encryptMode = Cipher.getInstance(transformation);
        encryptMode.init(Cipher.ENCRYPT_MODE, key);
        final Cipher decryptMode = Cipher.getInstance(transformation);
        decryptMode.init(Cipher.DECRYPT_MODE, key);
        return new Encryptor() {
            private Cipher encrypt = encryptMode;
            private Cipher decrypt = decryptMode;

            @Override
            public byte[] encrypt(byte[] input) {
                try {
                    return encrypt.doFinal(input);
                } catch (Throwable t) {
                    try {
                        encrypt = Cipher.getInstance(transformation);
                        encrypt.init(Cipher.ENCRYPT_MODE, key);
                    } catch (Exception ignored) { }
                    throw new RuntimeException(t);
                }
            }

            @Override
            public byte[] decrypt(byte[] input) {
                try {
                    return decrypt.doFinal(input);
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
     * Encrypts the input data.
     *
     * @param input the byte array to encrypt.
     * @return      an encrypted byte array.
     */
    byte[] encrypt(byte[] input);

    /**
     * Encrypts the input String data and return itself as byte array.
     *
     * @param input the String to encrypt.
     * @return      an encrypted String data.
     */
    default byte[] encryptUTF(@NotNull String input) {
        return encryptUTF(input, Charset.defaultCharset());
    }

    /**
     * Encrypts the input String data and return itself as byte array.
     *
     * @param input   the String to encrypt.
     * @param charset the Charset to encode the String.
     * @return        an encrypted String data.
     */
    default byte[] encryptUTF(@NotNull String input, @NotNull Charset charset) {
        return encrypt(input.getBytes(charset));
    }

    /**
     * Decrypts the input data.
     *
     * @param input the byte array to decrypt.
     * @return      a decrypted byte array.
     */
    byte[] decrypt(byte[] input);

    /**
     * Decrypts the input data and return itself as readable String.
     *
     * @param input the byte array to decrypt.
     * @return      a decrypted String.
     */
    @NotNull
    default String decryptUTF(byte[] input) {
        return decryptUTF(input, Charset.defaultCharset());
    }

    /**
     * Decrypts the input data and return itself as readable String.
     *
     * @param input   the byte array to decrypt.
     * @param charset the Charset to decode the String.
     * @return        a decrypted String.
     */
    @NotNull
    default String decryptUTF(byte[] input, @NotNull Charset charset) {
        return new String(decrypt(input), charset);
    }

    /**
     * Encrypts and writes the input String into the provided DataOutput.
     *
     * @param out   the DataOutput to write into.
     * @param input the String to encrypt and write.
     * @throws IOException if an I/O error occurs.
     */
    default void writeUTF(@NotNull DataOutput out, @NotNull String input) throws IOException {
        writeUTF(out, input, Charset.defaultCharset());
    }

    /**
     * Encrypts and writes the input String into the provided DataOutput.
     *
     * @param out     the DataOutput to write into.
     * @param input   the String to encrypt and write.
     * @param charset the Charset to encode the String.
     * @throws IOException if an I/O error occurs.
     */
    default void writeUTF(@NotNull DataOutput out, @NotNull String input, @NotNull Charset charset) throws IOException {
        final byte[] bytes = this.encryptUTF(input, charset);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    /**
     * Reads and decrypts a String from the provided DataInput.
     *
     * @param in the DataInput to read from.
     * @return   a decrypted String.
     * @throws IOException if an I/O error occurs.
     */
    @NotNull
    default String readUTF(@NotNull DataInput in) throws IOException {
        return readUTF(in, Charset.defaultCharset());
    }

    /**
     * Reads and decrypts a String from the provided DataInput.
     *
     * @param in      the DataInput to read from.
     * @param charset the Charset to decode the String.
     * @return        a decrypted String.
     * @throws IOException if an I/O error occurs.
     */
    @NotNull
    default String readUTF(@NotNull DataInput in, @NotNull Charset charset) throws IOException {
        final int length = in.readInt();
        final byte[] bytes = new byte[length];
        in.readFully(bytes);
        return this.decryptUTF(bytes, charset);
    }
}

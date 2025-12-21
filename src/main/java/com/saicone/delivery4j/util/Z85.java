/*
 * This file is part of gama, licensed under the MIT License
 *
 * Copyright (c) 2025 Rubenicos
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.saicone.delivery4j.util;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * A Java implementation of the Z85 encoding scheme, based on the <a href="https://rfc.zeromq.org/spec/32/">Z85 specification</a> used by ZeroMQ,
 * which is derivative of Ascii85 (and part of the Base85 family) encoding mechanism, but focused on source code usability.<br>
 * This implementation contains the following types of encoding and decoding:
 * <ul>
 * <li><a id="default"><b>Default</b></a>
 * <p> A Z85 encoding scheme that is almost the same as original Z85 specification, with key difference that mean to be used as a Base64 replacement.
 *     The encoder will add a {@code ~} character for every remainder to mark an encoded non-4-length array.
 *     The decoder will read every {@code ~} character to return the exact same array that was encoded.</p>
 * </li>
 * <li><a id="strict"><b>Strict</b></a>
 * <p> The original and restrictive Z85 encoding scheme without any change.
 *     The encoder will fail if provided array length is not a multiply of 4.
 *     The decoder will fail if provided String length is not a multiply of 5.</p>
 * </li>
 * <li><a id="padded"><b>Padded</b></a>
 * <p> A concise implementation that adds a padding for both array and String if the length does not meet Z85 scheme.
 *     The encoder will add empty bytes for arrays.
 *     The decoder will add {@code '0'} characters for Strings.</p>
 * </li>
 * </ul>
 *
 * Unlike {@link java.util.Base64}, this specification doesn't contain a url-safe variant.
 *
 * @author Rubenicos
 */
public class Z85 {

    /**
     * Z85 encoding alphabet, only the characters in this string are valid for Z85 encoding.
     */
    public static final String ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
    private static final byte[] INDEX = new byte[128];
    private static final byte NUL = '\0';
    private static final char MARK = '~';

    static {
        Arrays.fill(INDEX, (byte) 0xFF);
        for (int i = 0; i < ALPHABET.length(); i++) {
            INDEX[ALPHABET.charAt(i)] = (byte) i;
        }
    }

    private Z85() {
    }

    /**
     * Get a {@link Encoder} instance that encodes using the <a href="#default">Default</a> Z85 encoding scheme.
     *
     * @return a Z85 encoder.
     */
    @NotNull
    public static Encoder getEncoder() {
        return Encoder.INSTANCE;
    }

    /**
     * Get a {@link Encoder} instance that encodes using the <a href="#strict">Strict</a> Z85 encoding scheme.
     *
     * @return a Z85 encoder.
     */
    @NotNull
    public static Encoder getStrictEncoder() {
        return Encoder.STRICT;
    }

    /**
     * Get a {@link Encoder} instance that encodes using the <a href="#padded">Padded</a> Z85 encoding scheme.<br>
     * If an array length is not a multiple of 4, it is padded with empty bytes.
     *
     * @return a Z85 encoder.
     */
    @NotNull
    public static Encoder getPaddedEncoder() {
        return Encoder.PADDED;
    }

    /**
     * Get a {@link Decoder} instance that decodes using the <a href="#default">Default</a> Z85 encoding scheme.
     *
     * @return a Z85 decoder.
     */
    @NotNull
    public static Decoder getDecoder() {
        return Decoder.INSTANCE;
    }

    /**
     * Get a {@link Decoder} instance that decodes using the <a href="#strict">Strict</a> Z85 encoding scheme.
     *
     * @return a Z85 decoder.
     */
    @NotNull
    public static Decoder getStrictDecoder() {
        return Decoder.STRICT;
    }

    /**
     * Get a {@link Decoder} instance that decodes using the <a href="#padded">Padded</a> Z85 encoding scheme.<br>
     * If a String is not a multiple of 5, it is padded with {@code '\0'} characters.
     *
     * @return a Z85 decoder.
     */
    @NotNull
    public static Decoder getPaddedDecoder() {
        return Decoder.PADDED;
    }

    /**
     * This class provides methods to encode byte arrays into Z85 encoded Strings.
     */
    public static class Encoder {

        static final Encoder INSTANCE = new Encoder();
        static final Encoder STRICT = new Encoder() {
            @Override
            public @NotNull String encode(byte[] src) {
                if (src.length % 4 != 0) {
                    throw new IllegalArgumentException("Z85 encode: source length must be a multiple of 4");
                }
                return encode0(src);
            }
        };
        static final Encoder PADDED = new Encoder() {
            @Override
            public @NotNull String encode(byte[] src) {
                final int remainder = src.length % 4;
                if (remainder != 0) {
                    final int length = src.length + (4 - remainder);
                    src = Arrays.copyOf(src, length);
                }
                return encode0(src);
            }
        };

        /**
         * Encodes the specified byte array into a String using the Z85 encoding scheme.
         *
         * @param src the byte array to encode.
         * @return    a String containing the resulting Z85 encoded characters.
         */
        @NotNull
        public String encode(byte[] src) {
            final int remainder = src.length % 4;
            if (remainder == 0) {
                return encode0(src);
            }
            final int blocks = src.length / 4 + 1;
            final char[] out = new char[blocks * 5 + remainder];
            encode(src, out, blocks, remainder);
            for (int i = remainder; i > 0; i--) {
                out[out.length - i] = MARK;
            }
            return new String(out);
        }

        @NotNull
        String encode0(byte[] src) {
            final int blocks = src.length / 4;
            final char[] out = new char[blocks * 5];
            encode(src, out, blocks, 0);
            return new String(out);
        }

        void encode(byte[] in, char[] out, int blocks, int remainder) {
            byte b1 = NUL;
            byte b2 = NUL;
            byte b3 = NUL;
            byte b4 = NUL;

            int block = 0;
            int inPos = 0;
            int outPos = 0;
            try {
                while (block++ < blocks) {
                    // Next 4 bytes
                    b1 = in[inPos++];
                    b2 = in[inPos++];
                    b3 = in[inPos++];
                    b4 = in[inPos++];
                    outPos = encodeBlock(out, outPos, b1, b2, b3, b4);
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                if (remainder == 0 || block > blocks) {
                    throw e;
                } else if (remainder == 1) {
                    encodeBlock(out, outPos, b1, NUL, NUL, NUL);
                } else if (remainder == 2) {
                    encodeBlock(out, outPos, b1, b2, NUL, NUL);
                } else if (remainder == 3) {
                    encodeBlock(out, outPos, b1, b2, b3, NUL);
                }
            }
        }

        private int encodeBlock(char[] out, int outPos, byte b1, byte b2, byte b3, byte b4) {
            long value = ((b1 & 0xFFL) << 24)
                    | ((b2 & 0xFFL) << 16)
                    | ((b3 & 0xFFL) << 8)
                    | (b4 & 0xFFL);

            long c4 = value % 85L;
            value /= 85L;
            long c3 = value % 85L;
            value /= 85L;
            long c2 = value % 85L;
            value /= 85L;
            long c1 = value % 85L;
            value /= 85L;
            long c0 = value;

            out[outPos++] = ALPHABET.charAt((int) c0);
            out[outPos++] = ALPHABET.charAt((int) c1);
            out[outPos++] = ALPHABET.charAt((int) c2);
            out[outPos++] = ALPHABET.charAt((int) c3);
            out[outPos++] = ALPHABET.charAt((int) c4);

            return outPos;
        }
    }

    /**
     * This class provides methods to decode Z85 encoded Strings into byte arrays.
     */
    public static class Decoder {

        static final Decoder INSTANCE = new Decoder();
        static final Decoder STRICT = new Decoder() {
            @Override
            public byte[] decode(@NotNull String src) {
                if (src.length() % 5 != 0) {
                    throw new IllegalArgumentException("Z85 decode: source length must be a multiple of 5");
                }
                return decode0(src);
            }
        };
        static final Decoder PADDED = new Decoder() {
            @Override
            public byte[] decode(@NotNull String src) {
                final int remainder = src.length() % 5;
                if (remainder != 0) {
                    final int length = src.length() + (5 - remainder);
                    final StringBuilder padded = new StringBuilder(length);
                    padded.append(src);
                    for (int i = 0; i < (5 - remainder); i++) {
                        padded.append('0');
                    }
                    src = padded.toString();
                }
                return decode0(src);
            }
        };

        /**
         * Decodes a Z85 encoded String into a newly created byte array using the Z85 encoding scheme.
         *
         * @param src the string to decode.
         * @return    a newly created byte array containing the decoded bytes.
         */
        public byte[] decode(@NotNull String src) {
            final int remainder = src.length() % 5;
            if (remainder == 0) {
                return decode0(src);
            }
            final int blocks = src.length() / 5;
            final byte[] out = new byte[blocks * 4 - (4 - remainder)];
            decode(src, out, blocks);
            return out;
        }

        byte[] decode0(@NotNull String src) {
            final int blocks = src.length() / 5;
            final byte[] out = new byte[blocks * 4];
            decode(src, out, blocks);
            return out;
        }

        void decode(@NotNull String in, byte[] out, int blocks) {
            int inPos = 0;
            int outPos = 0;
            final byte[] index = INDEX;
            try {
                for (int block = 0; block < blocks; block++) {
                    // Next 5 chars
                    final int c0 = in.charAt(inPos++);
                    final int c1 = in.charAt(inPos++);
                    final int c2 = in.charAt(inPos++);
                    final int c3 = in.charAt(inPos++);
                    final int c4 = in.charAt(inPos++);

                    // Check range
                    if ((c0 | c1 | c2 | c3 | c4) >= 128) {
                        throw new IllegalArgumentException("Z85 decode: invalid character found near '" + in.substring(inPos - 5, inPos) + "'");
                    }

                    // Map to values using index
                    final byte v0 = index[c0];
                    final byte v1 = index[c1];
                    final byte v2 = index[c2];
                    final byte v3 = index[c3];
                    final byte v4 = index[c4];

                    // Validate values
                    if ((v0 | v1 | v2 | v3 | v4) == (byte) 0xFF) {
                        throw new IllegalArgumentException("Z85 decode: invalid character found near '" + in.substring(inPos - 5, inPos) + "'");
                    }

                    final long value = (v0 * 52200625L) // 85^4
                            + (v1 * 614125L) // 85^3
                            + (v2 * 7225L) // 85^2
                            + (v3 * 85L)
                            + v4;

                    out[outPos++] = (byte) ((value >> 24) & 0xFF);
                    out[outPos++] = (byte) ((value >> 16) & 0xFF);
                    out[outPos++] = (byte) ((value >> 8) & 0xFF);
                    out[outPos++] = (byte) (value & 0xFF);
                }
            } catch (ArrayIndexOutOfBoundsException ignored) {
                // expected exception
            }
        }
    }
}

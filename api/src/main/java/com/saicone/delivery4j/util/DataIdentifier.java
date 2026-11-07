package com.saicone.delivery4j.util;

import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An object to generate and identify messages with a unique id.
 *
 * @author Rubenicos
 */
public interface DataIdentifier {

    /**
     * Get a 32-bit data identifier.
     *
     * @return a 32-bit data identifier.
     */
    @NotNull
    static DataIdentifier bit32() {
        return new Bit32();
    }

    /**
     * Get a 64-bit data identifier.
     *
     * @return a 64-bit data identifier.
     */
    @NotNull
    static DataIdentifier bit64() {
        return new Bit64();
    }

    /**
     * Write a new unique id to the output.
     *
     * @param output the output to write the id.
     * @throws IOException if an I/O error occurs.
     */
    void write(@NotNull DataOutput output) throws IOException;

    /**
     * Read an id from the input and check if it doesn't belong to a generated identifier,
     * if it does, the id is invalid and should be ignored.
     *
     * @param input the input to read the id.
     * @return      true if the id is valid, false otherwise.
     * @throws IOException if an I/O error occurs.
     */
    boolean read(@NotNull DataInput input) throws IOException;

    /**
     * A 32-bit data identifier that generates unique integer ids.
     */
    class Bit32 implements DataIdentifier {

        private static final short MAGIC = (short) ThreadLocalRandom.current().nextInt();

        private final AtomicInteger current = new AtomicInteger(Short.MIN_VALUE);

        private short get() {
            return (short) current.get();
        }

        private short getAndIncrement() {
            return (short) current.getAndIncrement();
        }

        /**
         * Generate the next unique id.
         *
         * @return a new unique id.
         */
        public int next() {
            return ((MAGIC & 0xFFFF) << 16) | (getAndIncrement() & 0xFFFF);
        }

        /**
         * Check if the provided id belongs to a generated identifier.
         *
         * @param id the id to check.
         * @return   true if the id belongs to a generated identifier, false otherwise.
         */
        public boolean contains(int id) {
            if ((short) (id >>> 16) != MAGIC) {
                return false;
            }

            final short value = (short) id;
            final short current = get();
            final int diff = (current - value) & 0xFFFF;

            return diff <= 32767;
        }

        @Override
        public void write(@NotNull DataOutput output) throws IOException {
            output.writeInt(next());
        }

        @Override
        public boolean read(@NotNull DataInput input) throws IOException {
            return !contains(input.readInt());
        }
    }

    /**
     * A 64-bit data identifier that generates unique long ids.
     */
    class Bit64 implements DataIdentifier {

        private static final int MAGIC = ThreadLocalRandom.current().nextInt();

        private final AtomicInteger current = new AtomicInteger(Integer.MIN_VALUE);

        private int get() {
            return current.get();
        }

        private int getAndIncrement() {
            return current.getAndIncrement();
        }

        /**
         * Generate the next unique id.
         *
         * @return a new unique id.
         */
        public long next() {
            return ((long) MAGIC << 32) | (getAndIncrement() & 0xFFFFFFFFL);
        }

        /**
         * Check if the provided id belongs to a generated identifier.
         *
         * @param id the id to check.
         * @return   true if the id belongs to a generated identifier, false otherwise.
         */
        public boolean contains(long id) {
            if ((int) (id >>> 32) != MAGIC) {
                return false;
            }

            final int value = (int) id;
            final int current = get();
            final long diff = (current - value) & 0xFFFFFFFFL;

            return diff <= 0x7FFFFFFFL;
        }

        @Override
        public void write(@NotNull DataOutput output) throws IOException {
            output.writeLong(next());
        }

        @Override
        public boolean read(@NotNull DataInput input) throws IOException {
            return !contains(input.readLong());
        }
    }
}

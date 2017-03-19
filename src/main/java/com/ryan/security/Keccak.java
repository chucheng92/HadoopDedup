package com.ryan.security;

import com.ryan.util.LittleEndian;
import com.ryan.util.Parameters;

import java.util.Arrays;

/**
 * The Keccak digest algorithm. Instances of this class are not thread safe.
 *
 * @author Ryan Tao
 */

final class Keccak extends AbstractDigest {

    /**
     * round constants RC[i]
     */
    private static final long[] RC = new long[]{
            0x0000000000000001L, 0x0000000000008082L, 0x800000000000808aL,
            0x8000000080008000L, 0x000000000000808bL, 0x0000000080000001L,
            0x8000000080008081L, 0x8000000000008009L, 0x000000000000008aL,
            0x0000000000000088L, 0x0000000080008009L, 0x000000008000000aL,
            0x000000008000808bL, 0x800000000000008bL, 0x8000000000008089L,
            0x8000000000008003L, 0x8000000000008002L, 0x8000000000000080L,
            0x000000000000800aL, 0x800000008000000aL, 0x8000000080008081L,
            0x8000000000008080L, 0x0000000080000001L, 0x8000000080008008L
    };

    /**
     * Rotation offsets
     */
    private static final int[] R = new int[]{
            0, 1, 62, 28, 27, 36, 44, 6, 55, 20, 3, 10, 43,
            25, 39, 41, 45, 15, 21, 8, 18, 2, 61, 56, 14
    };

    private final long[] A;
    private final long[] B;
    private final long[] C;
    private final long[] D;
    private final int blockLen;
    private final byte[] buffer;
    private int inputOffset;

    /**
     * Creates a new ready to use {@code Keccak}.
     *
     * @param length the digest length (in bytes).
     * @throws IllegalArgumentException if {@code length} is not one of 28, 32, 48 or 64.
     */
    Keccak(int length) {
        super("Keccak-" + length * 8, length);
        Parameters.checkCondition(length == 28 || length == 32 || length == 48 || length == 64);
        this.A = new long[25];
        this.B = new long[25];
        this.C = new long[5];
        this.D = new long[5];
        this.blockLen = 200 - 2 * length; // Sponge R -> 144B(1152bit)
        this.buffer = new byte[blockLen];
        this.inputOffset = 0;
    }

    @Override
    public Digest reset() {
        for (int i = 0; i < 25; i++) {
            A[i] = 0L;
        }
        inputOffset = 0;
        return this;
    }

    @Override
    public Digest update(byte input) {
        buffer[inputOffset] = input;

        System.out.println(buffer);

        if (++inputOffset == blockLen) {
            processBuffer();
        }
        return this;
    }

    @Override
    public Digest update(byte[] input, int off, int len) {
        while (len > 0) {
            int cpLen = Math.min(blockLen - inputOffset, len);
            System.arraycopy(input, off, buffer, inputOffset, cpLen);
            inputOffset += cpLen;
            off += cpLen;
            len -= cpLen;
            if (inputOffset == blockLen) {
                processBuffer();
            }
        }
        return this;
    }

    @Override
    public byte[] digest() {
        addPadding();
        processBuffer();
        // Squeezing phase
        byte[] tmp = new byte[length() * 8];
        for (int i = 0; i < length(); i += 8) {
            LittleEndian.encode(A[i >>> 3], tmp, i);
        }
        reset();
        return Arrays.copyOf(tmp, length());
    }

    /**
     * Initialization and padding
     */
    private void addPadding() {
        if (inputOffset + 1 == buffer.length) {
            buffer[inputOffset] = (byte) 0x80;
        } else {
            buffer[inputOffset] = (byte) 0x06;
            for (int i = inputOffset + 1; i < buffer.length - 1; i++) {
                buffer[i] = 0;
            }
            buffer[buffer.length - 1] = (byte) 0x80;
        }
    }

    /**
     * Absorbing phase
     */
    private void processBuffer() {
        for (int i = 0; i < buffer.length; i += 8) {
            A[i >>> 3] ^= LittleEndian.decodeLong(buffer, i);
        }
        keccakf();
        inputOffset = 0;
    }

    /**
     * 24 rounds permutation
     */
    private void keccakf() {
        for (int n = 0; n < 24; n++) {
            round(n);
        }
    }

    /**
     * permutation
     *
     * @param n
     */
    private void round(int n) {
        // θ step
        for (int x = 0; x < 5; x++) {
            C[x] = A[index(x, 0)] ^ A[index(x, 1)] ^ A[index(x, 2)] ^ A[index(x, 3)] ^ A[index(x, 4)];
        }
        for (int x = 0; x < 5; x++) {
            D[x] = C[index(x - 1)] ^ rotate(C[index(x + 1)], 1);

        }
        for (int x = 0; x < 5; x++) {
            for (int y = 0; y < 5; y++) {
                A[index(x, y)] ^= D[x];
            }
        }
        // ρ and π steps
        for (int x = 0; x < 5; x++) {
            for (int y = 0; y < 5; y++) {
                int i = index(x, y);
                B[index(y, x * 2 + 3 * y)] = rotate(A[i], R[i]);
            }
        }
        // χ step
        for (int x = 0; x < 5; x++) {
            for (int y = 0; y < 5; y++) {
                int i = index(x, y);
                A[i] = B[i] ^ (~B[index(x + 1, y)] & B[index(x + 2, y)]);
            }
        }
        // ι step
        A[0] ^= RC[n];
    }

    private long rotate(long w, int r) {
        return Long.rotateLeft(w, r);
    }

    private int index(int x) {
        return x < 0 ? index(x + 5) : x % 5;
    }

    private int index(int x, int y) {
        return index(x) + 5 * index(y);
    }
}

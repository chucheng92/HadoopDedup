package com.ryan.security;

/**
 * Abstract skeleton implementation of the {@link Digest} interface
 *
 * @author Ryan Tao
 * @github lemonjing
 */
abstract class AbstractDigest implements Digest {
    private final String name;
    private final int length;

    /**
     * Create a new {@code AbstractDigest}
     *
     * @param length the digest's length in bytes.
     * @param name the digest algorithm's name.
     */
    AbstractDigest(String name, int length) {
        this.name = name;
        this.length = length;
    }

    /**
     * Returns the digest's length(in bytes)
     *
     * @return the digest's length(in bytes)
     */
    @Override
    public int length() {
        return length;
    }

    @Override
    public Digest update(byte... input) {
        return update(input, 0, input.length);
    }

    @Override
    public byte[] digest(byte... input) {
        return update(input).digest();
    }

    @Override
    public byte[] digest(byte[] input, int off, int len) {
        return update(input, off, len).digest();
    }

    @Override
    public String toString() {
        return name;
    }
}

package com.ryan.security;

import com.ryan.exception.CannotHappenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Some commonly used digest algorithms. None of the {@link Digest} instances
 * returned by this class is thread-safe.
 *
 * @author Ryan Tao
 * @github lemonjing
 */
public final class Digests {
    private static Logger logger = LoggerFactory.getLogger(Digests.class);

    private Digests() {
        /* ... */
    }

    /**
     * Returns a new MD5 {@code Digest} instance.
     *
     * @return a new MD5 {@code Digest} instance.
     */
    public static Digest md5() {
        return BuiltInDigest.create("MD5");
    }

    /**
     * Returns a new SHA1 {@code Digest} instance.
     *
     * @return a new SHA1 {@code Digest} instance.
     */
    public static Digest sha1() {
        return BuiltInDigest.create("SHA1");
    }

    /**
     * Returns a new SHA-256 {@code Digest} instance.
     *
     * @return a new SHA-256 {@code Digest} instance.
     */
    public static Digest sha256() {
        return BuiltInDigest.create("SHA-256");
    }

    /**
     * Returns a new SHA-512 {@code Digest} instance.
     *
     * @return a new SHA-512 {@code Digest} instance.
     */
    public static Digest sha512() {
        return BuiltInDigest.create("SHA-512");
    }

    /**
     * Returns a new Keccak-224 {@code Digest} instance.
     *
     * @return a new Keccak-224 {@code Digest} instance.
     */
    public static Digest keccak224() {
        return new Keccak(28);
    }

    /**
     * Returns a new Keccak-256 {@code Digest} instance.
     *
     * @return a new Keccak-256 {@code Digest} instance.
     */
    public static Digest keccak256() {
        return new Keccak(32);
    }

    /**
     * Returns a new Keccak-384 {@code Digest} instance.
     *
     * @return a new Keccak-384 {@code Digest} instance.
     */
    public static Digest keccak384() {
        return new Keccak(48);
    }

    /**
     * Returns a new Keccak-512 {@code Digest} instance.
     *
     * @return a new Keccak-512 {@code Digest} instance.
     */
    public static Digest keccak512() {
        return new Keccak(64);
    }

    private static final class BuiltInDigest extends AbstractDigest {
        static Digest create(String algorithm) {
            MessageDigest md;
            try {
                md = MessageDigest.getInstance(algorithm);
            } catch (NoSuchAlgorithmException e) {
                throw new CannotHappenException(e);
            }
            return new BuiltInDigest(md);
        }

        private final MessageDigest md;

        private BuiltInDigest(MessageDigest md) {
            super(md.getAlgorithm(), md.getDigestLength());
            this.md = md;
        }

        @Override
        public Digest reset() {
            md.reset();
            return this;
        }

        @Override
        public Digest update(byte input) {
            md.update(input);
            return this;
        }

        @Override
        public Digest update(byte[] input, int offset, int len) {
            md.update(input, offset, len);
            return this;
        }

        @Override
        public byte[] digest() {
            return md.digest();
        }
    }
}

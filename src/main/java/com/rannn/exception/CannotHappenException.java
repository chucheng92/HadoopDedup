package com.rannn.exception;

/**
 * This exception is meant to be thrown in situations
 * that can't happen in practice.
 *
 * @author Rannn Tao
 * @github lemonjing
 */
public final class CannotHappenException extends RuntimeException {
    private static final long serialVersionUID = -5243066326988754946L;

    /**
     * Create a new {@code CannotHappenException}.
     */
    public CannotHappenException() {
        /* ... */
    }

    /**
     * Create a new {@code CannotHappenException}.
     *
     * @param message the error message
     */
    public CannotHappenException(String message) {
        super(message);
    }

    /**
     * Create a new {@code CannotHappenException}.
     *
     * @param cause the error that caused this one
     */
    public CannotHappenException(Throwable cause) {
        super(cause);
    }

    /**
     * Create a new {@code CannotHappenException}.
     *
     * @param message the error message
     * @param cause the error that caused this one
     */
    public CannotHappenException(String message, Throwable cause) {
        super(message, cause);
    }
}

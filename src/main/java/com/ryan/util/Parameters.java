package com.ryan.util;

/**
 * Utility class to test parameters validity.
 *
 * @author Osman KOCAK
 */
public final class Parameters {
    /**
     * Checks that the given reference is not {@code null} and returns it
     * in case of success.
     *
     * @param <T> the type of the given reference.
     * @param ref the reference to test.
     * @return the validated (non-{@code null}) reference.
     * @throws NullPointerException if the given reference is {@code null}.
     */
    public static <T> T checkNotNull(T ref) {
        if (ref == null) {
            throw new NullPointerException();
        }
        return ref;
    }

    /**
     * Checks that the given reference is not {@code null} and returns it
     * in case of success.
     *
     * @param <T>  the type of the given reference.
     * @param ref  the reference to test.
     * @param msg  the error message format string.
     * @param args the error message arguments.
     * @return the validated (non-{@code null}) reference.
     * @throws NullPointerException if the given reference is {@code null}.
     */
    public static <T> T checkNotNull(T ref, String msg, Object... args) {
        if (ref == null) {
            throw new NullPointerException(format(msg, args));
        }
        return ref;
    }

    /**
     * Checks the truth of the given condition checking parameters validity.
     *
     * @param condition the boolean condition to test.
     * @throws IllegalArgumentException if the condition is {@code false}.
     */
    public static void checkCondition(boolean condition) {
        if (!condition) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Checks the truth of the given condition checking parameters validity.
     *
     * @param condition the boolean condition to test.
     * @param msg       the error message format string.
     * @param args      the error message arguments.
     * @throws IllegalArgumentException if the condition is {@code false}.
     */
    public static void checkCondition(boolean condition, String msg,
                                      Object... args) {
        if (!condition) {
            throw new IllegalArgumentException(format(msg, args));
        }
    }

    /**
     * Checks the type of the given reference and, in case of success, casts
     * and returns it.
     *
     * @param <T>  the expected type of the given reference.
     * @param ref  the reference to test.
     * @param type the expected type of the given reference.
     * @return the given reference in the expected type.
     * @throws NullPointerException if {@code type} is {@code null}.
     * @throws ClassCastException   if the given reference doesn't represent
     *                              an instance of the expected class.
     */
    public static <T> T checkType(Object ref, Class<T> type) {
        if (type.isInstance(ref)) {
            return type.cast(ref);
        }
        throw new ClassCastException();
    }

    /**
     * Checks the type of the given reference and, in case of success, casts
     * and returns it.
     *
     * @param <T>  the expected type of the given reference.
     * @param ref  the reference to test.
     * @param type the expected type of the given reference.
     * @param msg  the error message format string.
     * @param args the error message arguments.
     * @return the given reference in the expected type.
     * @throws NullPointerException if {@code type} is {@code null}.
     * @throws ClassCastException   if the given reference doesn't represent
     *                              an instance of the given class.
     */
    public static <T> T checkType(Object ref, Class<T> type, String msg,
                                  Object... args) {
        if (type.isInstance(ref)) {
            return type.cast(ref);
        }
        throw new ClassCastException(format(msg, args));
    }

    private static String format(String format, Object... args) {
        if (format == null) {
            return null;
        }
        return args != null ? String.format(format, args) : format;
    }

    private Parameters() {
        /* ... */
    }
}

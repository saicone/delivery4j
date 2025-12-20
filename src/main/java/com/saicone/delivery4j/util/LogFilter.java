package com.saicone.delivery4j.util;

import org.apache.logging.log4j.LogManager;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A minimalist representation of a Logger implementation with a small amount of logging levels.<br>
 * Unlike normal logger implementations, this one uses numbers as levels:<br>
 * 1 = ERROR / SEVERE<br>
 * 2 = WARNING<br>
 * 3 = INFO<br>
 * 4 = DEBUG INFORMATION
 *
 * @author Rubenicos
 */
public interface LogFilter {

    /**
     * Error logging level.<br>
     * This level generally represent information that the logger must receive
     * since an error have important consequences on the running code.
     */
    int ERROR = 1;
    /**
     * Warning logging level.<br>
     * Similar importance as error, but the information that offer this level
     * mean that even though the running code encountered an error, it can continue.
     */
    int WARNING = 2;
    /**
     * Info logging level.<br>
     * Just one-time information that can be skipped and offer a general
     * state of the running code.
     */
    int INFO = 3;
    /**
     * Debug logging level.<br>
     * Completely unnecessary information that is only useful when you want
     * to track a problem in the code by providing a view of the running
     * application as most descriptive way possible.
     */
    int DEBUG = 4;

    /**
     * Create a LogFilter instance for the given class, using the best
     * available logging implementation found on classpath.
     *
     * @param clazz    the class to create LogFilter for.
     * @param maxLevel the maximum logging level.
     * @return         a newly generated LogFilter instance.
     */
    @NotNull
    static LogFilter valueOf(@NotNull Class<?> clazz, @MagicConstant(valuesFromClass = LogFilter.class) int maxLevel) {
        return valueOf(clazz, () -> maxLevel);
    }

    /**
     * Create a LogFilter instance for the given class, using the best
     * available logging implementation found on classpath.
     *
     * @param clazz    the class to create LogFilter for.
     * @param maxLevel the maximum logging level supplier.
     * @return         a newly generated LogFilter instance.
     */
    @NotNull
    static LogFilter valueOf(@NotNull Class<?> clazz, @NotNull Supplier<Integer> maxLevel) {
        Objects.requireNonNull(clazz, "Cannot create LogFilter for null class");
        Objects.requireNonNull(maxLevel, "Cannot create LogFilter using null max level supplier");

        // log4j
        try {
            Class.forName("org.apache.logging.log4j.Logger");
            return new Log4j(LogManager.getLogger(clazz), maxLevel);
        } catch (Throwable ignored) { }

        // slf4j
        try {
            Class.forName("org.slf4j.Logger");
            return new Slf4j(LoggerFactory.getLogger(clazz), maxLevel);
        } catch (Throwable ignored) { }

        // fallback
        return new Java(Logger.getLogger(clazz.getName()), maxLevel);
    }

    /**
     * Create a LogFilter instance for the given name, using the best
     * available logging implementation found on classpath.
     *
     * @param name     the name to create LogFilter for.
     * @param maxLevel the maximum logging level.
     * @return         a newly generated LogFilter instance.
     */
    @NotNull
    static LogFilter valueOf(@NotNull String name, @MagicConstant(valuesFromClass = LogFilter.class) int maxLevel) {
        return valueOf(name, () -> maxLevel);
    }

    /**
     * Create a LogFilter instance for the given name, using the best
     * available logging implementation found on classpath.
     *
     * @param name     the name to create LogFilter for.
     * @param maxLevel the maximum logging level supplier.
     * @return         a newly generated LogFilter instance.
     */
    @NotNull
    static LogFilter valueOf(@NotNull String name, @NotNull Supplier<Integer> maxLevel) {
        Objects.requireNonNull(name, "Cannot create LogFilter for null name");
        Objects.requireNonNull(maxLevel, "Cannot create LogFilter using null max level supplier");

        // log4j
        try {
            Class.forName("org.apache.logging.log4j.Logger");
            return new Log4j(LogManager.getLogger(name), maxLevel);
        } catch (Throwable ignored) { }

        // slf4j
        try {
            Class.forName("org.slf4j.Logger");
            return new Slf4j(LoggerFactory.getLogger(name), maxLevel);
        } catch (Throwable ignored) { }

        // fallback
        return new Java(Logger.getLogger(name), maxLevel);
    }

    /**
     * Create a LogFilter instance for the given object, using the best
     * available logging implementation found on classpath.<br>
     * The object can be of type Class, String or a Logger instance itself.
     *
     * @param object   the object to create LogFilter for.
     * @param maxLevel the maximum logging level.
     * @return         a newly generated LogFilter instance.
     */
    @NotNull
    static LogFilter valueOf(@NotNull Object object, @MagicConstant(valuesFromClass = LogFilter.class) int maxLevel) {
        return valueOf(object, () -> maxLevel);
    }

    /**
     * Create a LogFilter instance for the given object, using the best
     * available logging implementation found on classpath.<br>
     * The object can be of type Class, String or a Logger instance itself.
     *
     * @param object   the object to create LogFilter for.
     * @param maxLevel the maximum logging level supplier.
     * @return         a newly generated LogFilter instance.
     */
    @NotNull
    static LogFilter valueOf(@NotNull Object object, @NotNull Supplier<Integer> maxLevel) {
        Objects.requireNonNull(object, "Cannot convert null object into LogFilter");
        Objects.requireNonNull(maxLevel, "Cannot create LogFilter using null max level supplier");

        if (object instanceof Class) {
            return valueOf((Class<?>) object, maxLevel);
        } else if (object instanceof String) {
            return valueOf((String) object, maxLevel);
        } else if (object instanceof Logger) {
            return new Java((Logger) object, maxLevel);
        }

        // log4j
        try {
            final Class<?> type = Class.forName("org.apache.logging.log4j.Logger");
            if (type.isInstance(object)) {
                return new Log4j((org.apache.logging.log4j.Logger) object, maxLevel);
            }
        } catch (Throwable ignored) { }

        // slf4j
        try {
            final Class<?> type = Class.forName("org.slf4j.Logger");
            if (type.isInstance(object)) {
                return new Slf4j((org.slf4j.Logger) object, maxLevel);
            }
        } catch (Throwable ignored) { }

        throw new IllegalArgumentException("The object type " + object.getClass() + " cannot be converted into LogFilter");
    }

    /**
     * The maximum logging level accepted by the used logger implementation.
     *
     * @return a logging level, as described on {@link LogFilter}.
     */
    @MagicConstant(valuesFromClass = LogFilter.class)
    int maxLevel();

    /**
     * Log a message.
     *
     * @param level the message level type.
     * @param msg   the message to log.
     */
    void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull String msg);

    /**
     * Log a message, with associated Throwable information.
     *
     * @param level     the message level type.
     * @param msg       the message to log.
     * @param throwable the Throwable associated with log message.
     */
    void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull String msg, @NotNull Throwable throwable);

    /**
     * Log a message, which is only to be constructed if the logging level is allowed by current implementation.
     *
     * @param level the message level type.
     * @param msg   the message to log.
     */
    default void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull Supplier<String> msg) {
        log(level, msg.get());
    }

    /**
     * Log a message, which is only to be constructed if the logging level is allowed by current implementation.
     *
     * @param level     the message level type.
     * @param msg       the message to log.
     * @param throwable the Throwable associated with log message.
     */
    default void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull Supplier<String> msg, @NotNull Throwable throwable) {
        log(level, msg.get(), throwable);
    }

    /**
     * Java Util Logging implementation of LogFilter.
     */
    class Java implements LogFilter {

        private static final Level[] LEVELS = new Level[] {
                null,
                Level.SEVERE,
                Level.WARNING,
                Level.INFO,
                Level.INFO
        };

        private final Logger logger;
        private final Supplier<Integer> maxLevel;

        /**
         * Constructor of Java LogFilter.
         *
         * @param logger   the Java Util Logging logger.
         * @param maxLevel the maximum logging level supplier.
         */
        public Java(@NotNull Logger logger, @NotNull Supplier<Integer> maxLevel) {
            this.logger = logger;
            this.maxLevel = maxLevel;
        }

        /**
         * Get the underlying Java Util Logging logger.
         *
         * @return the Java Util Logging logger.
         */
        @NotNull
        public Logger logger() {
            return logger;
        }

        @Override
        public int maxLevel() {
            return maxLevel.get();
        }

        @Override
        public void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull String msg) {
            if (level > maxLevel()) {
                return;
            }
            this.logger.log(LEVELS[level], msg);
        }

        @Override
        public void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull String msg, @NotNull Throwable throwable) {
            if (level > maxLevel()) {
                return;
            }
            this.logger.log(LEVELS[level], msg, throwable);
        }

        @Override
        public void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull Supplier<String> msg) {
            if (level > maxLevel()) {
                return;
            }
            this.logger.log(LEVELS[level], msg);
        }

        @Override
        public void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull Supplier<String> msg, @NotNull Throwable throwable) {
            if (level > maxLevel()) {
                return;
            }
            this.logger.log(LEVELS[level], throwable, msg);
        }
    }

    /**
     * Log4j2 implementation of LogFilter.
     */
    class Log4j implements LogFilter {

        private static final org.apache.logging.log4j.Level[] LEVELS = new org.apache.logging.log4j.Level[] {
                null,
                org.apache.logging.log4j.Level.ERROR,
                org.apache.logging.log4j.Level.WARN,
                org.apache.logging.log4j.Level.INFO,
                org.apache.logging.log4j.Level.INFO
        };

        private final org.apache.logging.log4j.Logger logger;
        private final Supplier<Integer> maxLevel;

        /**
         * Constructor of Log4j LogFilter.
         *
         * @param logger   the Log4j2 logger.
         * @param maxLevel the maximum logging level supplier.
         */
        public Log4j(@NotNull org.apache.logging.log4j.Logger logger, @NotNull Supplier<Integer> maxLevel) {
            this.logger = logger;
            this.maxLevel = maxLevel;
        }

        /**
         * Get the underlying Log4j2 logger.
         *
         * @return the Log4j2 logger.
         */
        @NotNull
        public org.apache.logging.log4j.Logger logger() {
            return logger;
        }

        @Override
        public int maxLevel() {
            return maxLevel.get();
        }

        @Override
        public void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull String msg) {
            if (level > maxLevel()) {
                return;
            }
            this.logger.log(LEVELS[level], msg);
        }

        @Override
        public void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull String msg, @NotNull Throwable throwable) {
            if (level > maxLevel()) {
                return;
            }
            this.logger.log(LEVELS[level], msg, throwable);
        }

        @Override
        public void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull Supplier<String> msg) {
            if (level > maxLevel()) {
                return;
            }
            this.logger.log(LEVELS[level], msg);
        }

        @Override
        public void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull Supplier<String> msg, @NotNull Throwable throwable) {
            if (level > maxLevel()) {
                return;
            }
            this.logger.log(LEVELS[level], msg, throwable);
        }
    }

    /**
     * SLF4J implementation of LogFilter.
     */
    class Slf4j implements LogFilter {

        private final org.slf4j.Logger logger;
        private final Supplier<Integer> maxLevel;

        /**
         * Constructor of SLF4J LogFilter.
         *
         * @param logger   the SLF4J logger.
         * @param maxLevel the maximum logging level supplier.
         */
        public Slf4j(@NotNull org.slf4j.Logger logger, @NotNull Supplier<Integer> maxLevel) {
            this.logger = logger;
            this.maxLevel = maxLevel;
        }

        /**
         * Get the underlying SLF4J logger.
         *
         * @return the SLF4J logger.
         */
        @NotNull
        public org.slf4j.Logger logger() {
            return logger;
        }

        @Override
        public int maxLevel() {
            return maxLevel.get();
        }

        @Override
        public void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull String msg) {
            if (level > maxLevel()) {
                return;
            }
            log0(level, msg);
        }

        @Override
        public void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull String msg, @NotNull Throwable throwable) {
            if (level > maxLevel()) {
                return;
            }
            log0(level, msg, throwable);
        }

        @Override
        public void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull Supplier<String> msg) {
            if (level > maxLevel()) {
                return;
            }
            log0(level, msg.get());
        }

        @Override
        public void log(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull Supplier<String> msg, @NotNull Throwable throwable) {
            if (level > maxLevel()) {
                return;
            }
            log0(level, msg.get(), throwable);
        }

        private void log0(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull String msg) {
            switch (level) {
                case ERROR:
                    this.logger.error(msg);
                    break;
                case WARNING:
                    this.logger.warn(msg);
                    break;
                case INFO:
                case DEBUG:
                    this.logger.info(msg);
                    break;
                default:
                    throw new IllegalArgumentException("The logging level '" + level + "' does not exist");
            }
        }

        private void log0(@MagicConstant(valuesFromClass = LogFilter.class) int level, @NotNull String msg, @NotNull Throwable throwable) {
            switch (level) {
                case ERROR:
                    this.logger.error(msg, throwable);
                    break;
                case WARNING:
                    this.logger.warn(msg, throwable);
                    break;
                case INFO:
                case DEBUG:
                    this.logger.info(msg, throwable);
                    break;
                default:
                    throw new IllegalArgumentException("The logging level '" + level + "' does not exist");
            }
        }
    }
}

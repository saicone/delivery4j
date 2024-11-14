package com.saicone.delivery4j.log;

import com.saicone.delivery4j.Broker;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Log4j integration for broker logging.
 *
 * @author Rubenicos
 */
public class Log4jLogger implements Broker.Logger {

    private final Logger logger;

    /**
     * Create a log4j logger to provided class.
     *
     * @param clazz the class owning the logger.
     */
    public Log4jLogger(@NotNull Class<?> clazz) {
        this.logger = LogManager.getLogger(clazz);
    }

    private void log(int level, @NotNull Consumer<Level> consumer) {
        switch (level) {
            case 1:
                consumer.accept(Level.ERROR);
                break;
            case 2:
                consumer.accept(Level.WARN);
                break;
            case 3:
                consumer.accept(Level.INFO);
                break;
            case 4:
            default:
                if (DEBUG) {
                    consumer.accept(Level.INFO);
                }
                break;
        }
    }

    @Override
    public void log(int level, @NotNull String msg) {
        log(level, lvl -> this.logger.log(lvl, msg));
    }

    @Override
    public void log(int level, @NotNull String msg, @NotNull Throwable throwable) {
        log(level, lvl -> this.logger.log(lvl, msg, throwable));
    }

    @Override
    public void log(int level, @NotNull Supplier<String> msg) {
        log(level, lvl -> this.logger.log(lvl, msg));
    }

    @Override
    public void log(int level, @NotNull Supplier<String> msg, @NotNull Throwable throwable) {
        log(level, lvl -> this.logger.log(lvl, msg, throwable));
    }
}

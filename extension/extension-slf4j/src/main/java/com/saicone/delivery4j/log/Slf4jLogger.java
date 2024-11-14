package com.saicone.delivery4j.log;

import com.saicone.delivery4j.Broker;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Slf4j integration for broker logging.
 *
 * @author Rubenicos
 */
public class Slf4jLogger implements Broker.Logger {

    private final Logger logger;

    /**
     * Create a slf4j logger to provided class.
     *
     * @param clazz the class owning the logger.
     */
    public Slf4jLogger(@NotNull Class<?> clazz) {
        this.logger = LoggerFactory.getLogger(clazz);
    }

    @Override
    public void log(int level, @NotNull String msg) {
        switch (level) {
            case 1:
                this.logger.error(msg);
                break;
            case 2:
                this.logger.warn(msg);
                break;
            case 3:
                this.logger.info(msg);
                break;
            case 4:
            default:
                if (DEBUG) {
                    this.logger.info(msg);
                }
                break;
        }
    }

    @Override
    public void log(int level, @NotNull String msg, @NotNull Throwable throwable) {
        switch (level) {
            case 1:
                this.logger.error(msg, throwable);
                break;
            case 2:
                this.logger.warn(msg, throwable);
                break;
            case 3:
                this.logger.info(msg, throwable);
                break;
            case 4:
            default:
                if (DEBUG) {
                    this.logger.info(msg, throwable);
                }
                break;
        }
    }
}

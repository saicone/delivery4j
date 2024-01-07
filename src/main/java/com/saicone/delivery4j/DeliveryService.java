package com.saicone.delivery4j;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * Delivery service interface to supply methods for delivery clients.
 * <p>
 * Also represents an operation that accepts a channel ID with byte array data and returns boolean
 * depending on an effective reception with the functional method {@link #receive(String, byte[])}.
 * <p>
 * Unlike a normal functional interface, the delivery service offers methods like {@link #log(int, Throwable)} and {@link #log(int, String)}
 * for logging integration, also with {@link #async(Runnable)} and {@link #asyncRepeating(Runnable, long, TimeUnit)} for async execution service integration.
 *
 * @author Rubenicos
 */
@FunctionalInterface
public interface DeliveryService {

    /**
     * Receive a byte array data associated with channel ID.
     *
     * @param channel the channel ID.
     * @param data    the byte array data to receive.
     * @return        true if the data was effectively received.
     */
    boolean receive(String channel, byte[] data);

    /**
     * Logging integration for {@link Throwable} object, using logging levels:<br>
     * 1 = error<br>
     * 2 = warning<br>
     * 3 = info<br>
     * 4 = debug<br>
     * <br>
     * It's strongly suggested to override this method with a more robust logging system.
     *
     * @param level the logging level.
     * @param t     the exception to log.
     */
    default void log(int level, @NotNull Throwable t) {
        t.printStackTrace();
    }

    /**
     * Logging integration for text message, using logging levels:<br>
     * 1 = error<br>
     * 2 = warning<br>
     * 3 = info<br>
     * 4 = debug<br>
     * <br>
     * It's strongly suggested to override this method with a more robust logging system.
     *
     * @param level the logging level.
     * @param msg   the message to log.
     */
    default void log(int level, @NotNull String msg) {
        System.out.println(msg);
    }

    /**
     * Async execution integration to run a task asynchronously.
     *
     * @param runnable the task to run.
     * @return         a {@link Runnable} object to stop the executed task.
     */
    default @NotNull Runnable async(@NotNull Runnable runnable) {
        final Thread thread = new Thread(runnable);
        thread.start();
        return thread::interrupt;
    }

    /**
     * Async execution integration to repeat a task asynchronously.
     *
     * @param runnable the task to run.
     * @param time     the time interval.
     * @param unit     the unit of interval.
     * @return         a {@link Runnable} object to stop the executed task.
     */
    @SuppressWarnings("all")
    default @NotNull Runnable asyncRepeating(@NotNull Runnable runnable, long time, @NotNull TimeUnit unit) {
        final Thread thread = new Thread(() -> {
            while (!Thread.interrupted()) {
                runnable.run();
                try {
                    Thread.sleep(unit.toMillis(time));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        thread.start();
        return thread::interrupt;
    }

}

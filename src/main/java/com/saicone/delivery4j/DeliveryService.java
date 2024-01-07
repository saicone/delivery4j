package com.saicone.delivery4j;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

@FunctionalInterface
public interface DeliveryService {

    boolean receive(String channel, byte[] data);

    default void log(int level, @NotNull Throwable t) {
        // empty default method
    }

    default void log(int level, @NotNull String msg) {
        // empty default method
    }

    default @NotNull Runnable async(@NotNull Runnable runnable) {
        final Thread thread = new Thread(runnable);
        thread.start();
        return thread::interrupt;
    }

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

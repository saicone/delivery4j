package com.saicone.delivery4j;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@FunctionalInterface
public interface DeliveryService {

    boolean receive(String channel, byte[] data);

    default void log(@NotNull Throwable t) {
        // empty default method
    }

    default void log(int level, @NotNull String msg) {
        // empty default method
    }

    @NotNull
    default Consumer<Boolean> async(@NotNull Runnable runnable) {
        final Thread thread = new Thread(runnable);
        thread.start();
        return bool -> {
            if (bool == null) {
                thread.interrupt();
                thread.start();
            } else if (!bool) {
                thread.interrupt();
            } else if (!thread.isAlive()) {
                thread.start();
            }
        };
    }

    @NotNull
    @SuppressWarnings("all")
    default Consumer<Boolean> asyncRepeating(@NotNull Runnable runnable, long time, @NotNull TimeUnit unit) {
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
        return bool -> {
            if (bool == null) {
                thread.interrupt();
                thread.start();
            } else if (!bool) {
                thread.interrupt();
            } else if (!thread.isAlive()) {
                thread.start();
            }
        };
    }

}

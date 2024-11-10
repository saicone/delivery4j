package com.saicone.delivery4j.util;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public interface DelayedExecutor<T> {

    DelayedExecutor<Thread> JAVA = new DelayedExecutor<>() {
        @Override
        public @NotNull Thread execute(@NotNull Runnable command) {
            final Thread thread = new Thread(command);
            thread.start();
            return thread;
        }

        @Override
        public @NotNull Thread execute(@NotNull Runnable command, long delay, @NotNull TimeUnit unit) {
            final Thread thread = new Thread(() -> {
                try {
                    Thread.sleep(unit.toMillis(delay));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (!Thread.interrupted()) {
                    command.run();
                }
            });
            thread.start();
            return thread;
        }

        @Override
        public @NotNull Thread execute(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit) {
            final Thread thread = new Thread(() -> {
                if (delay > 0) {
                    try {
                        Thread.sleep(unit.toMillis(delay));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                while (!Thread.interrupted()) {
                    command.run();
                    try {
                        Thread.sleep(unit.toMillis(period));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            thread.start();
            return thread;
        }

        @Override
        public void cancel(@NotNull Thread thread) {
            thread.interrupt();
        }
    };

    @NotNull
    T execute(@NotNull Runnable command);

    @NotNull
    T execute(@NotNull Runnable command, long delay, @NotNull TimeUnit unit);

    @NotNull
    T execute(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit);

    void cancel(@NotNull T t);

    @NotNull
    default Executor asExecutor() {
        return this::execute;
    }
}

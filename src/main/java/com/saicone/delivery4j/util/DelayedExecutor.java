package com.saicone.delivery4j.util;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public interface DelayedExecutor<T> extends Executor {

    DelayedExecutor<Thread> JAVA = new DelayedExecutor<>() {
        @Override
        public void execute(@NotNull Runnable command) {
            new Thread(command).start();
        }

        @Override
        public @NotNull Thread execute(@NotNull Runnable command, long delay, @NotNull TimeUnit unit) {
            return new Thread(() -> {
                try {
                    Thread.sleep(unit.toMillis(delay));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (!Thread.interrupted()) {
                    command.run();
                }
            });
        }

        @Override
        public @NotNull Thread execute(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit) {
            return new Thread(() -> {
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
        }

        @Override
        public void cancel(@NotNull Thread thread) {
            thread.interrupt();
        }
    };

    @NotNull
    T execute(@NotNull Runnable command, long delay, @NotNull TimeUnit unit);

    @NotNull
    T execute(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit);

    void cancel(@NotNull T t);
}

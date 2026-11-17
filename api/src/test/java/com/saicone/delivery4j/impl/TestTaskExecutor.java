package com.saicone.delivery4j.impl;

import com.saicone.delivery4j.util.TaskExecutor;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

public class TestTaskExecutor implements TaskExecutor<Void> {

    public static final TestTaskExecutor INSTANCE = new TestTaskExecutor();

    @Override
    public @NotNull Void run(@NotNull Runnable command) {
        command.run();
        return null;
    }

    @Override
    public @NotNull Void runLater(@NotNull Runnable command, long delay, @NotNull TimeUnit unit) {
        command.run();
        return null;
    }

    @Override
    public @NotNull Void runTimer(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit) {
        command.run();
        return null;
    }

    @Override
    public void cancel(@NotNull Void unused) {
        // empty method
    }
}

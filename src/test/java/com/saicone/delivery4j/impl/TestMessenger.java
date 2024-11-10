package com.saicone.delivery4j.impl;

import com.saicone.delivery4j.AbstractMessenger;
import com.saicone.delivery4j.Broker;
import com.saicone.delivery4j.broker.TestBroker;
import com.saicone.delivery4j.util.DelayedExecutor;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

public class TestMessenger extends AbstractMessenger implements DelayedExecutor<Void> {

    public TestMessenger() {
        setExecutor(this.asExecutor());
    }

    @Override
    protected @NotNull Broker<?> loadBroker() {
        return new TestBroker();
    }

    @Override
    public @NotNull Void execute(@NotNull Runnable command) {
        command.run();
        return null;
    }

    @Override
    public @NotNull Void execute(@NotNull Runnable command, long delay, @NotNull TimeUnit unit) {
        command.run();
        return null;
    }

    @Override
    public @NotNull Void execute(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit) {
        command.run();
        return null;
    }

    @Override
    public void cancel(@NotNull Void unused) {
        // empty method
    }
}

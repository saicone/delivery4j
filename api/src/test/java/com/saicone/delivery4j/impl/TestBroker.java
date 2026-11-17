package com.saicone.delivery4j.impl;

import com.saicone.delivery4j.Broker;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class TestBroker extends Broker {

    public TestBroker() {
        setExecutor(TestTaskExecutor.INSTANCE);
    }

    @Override
    protected void onStart() {
        setEnabled(true);
    }

    @Override
    public void send(@NotNull String channel, byte[] data) throws IOException {
        receive(channel, data);
    }
}

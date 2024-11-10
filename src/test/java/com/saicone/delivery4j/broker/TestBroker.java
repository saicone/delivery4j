package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.Broker;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class TestBroker extends Broker<TestBroker> {

    @Override
    protected @NotNull TestBroker get() {
        return this;
    }

    @Override
    protected void onStart() {
        setEnabled(true);
    }

    @Override
    protected void onSend(@NotNull String channel, byte[] data) throws IOException {
        receive(channel, data);
    }
}

package com.saicone.delivery4j.impl;

import com.saicone.delivery4j.message.AbstractMessenger;
import com.saicone.delivery4j.Broker;
import org.jetbrains.annotations.NotNull;

public class TestMessenger extends AbstractMessenger {

    @Override
    protected @NotNull Broker loadBroker() {
        final TestBroker broker = new TestBroker();
        broker.setExecutor(TestTaskExecutor.INSTANCE);
        return broker;
    }
}

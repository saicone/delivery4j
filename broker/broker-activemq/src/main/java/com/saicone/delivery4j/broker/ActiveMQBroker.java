package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.Broker;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class ActiveMQBroker extends Broker {

    private final Connection connection;

    private Session session;

    private final Map<String, Bridge> bridges = new HashMap<>();

    @NotNull
    public static ActiveMQBroker of(@NotNull Consumer<ActiveMQConnectionFactory> consumer) throws JMSException {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        consumer.accept(factory);
        return new ActiveMQBroker(factory.createConnection());
    }

    public ActiveMQBroker(@NotNull Connection connection) {
        this.connection = connection;
    }

    @Override
    protected void onStart() {
        try {
            this.connection.start();

            this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            for (String channel : getSubscribedChannels()) {
                this.bridges.put(channel, new Bridge(channel));
            }
        } catch (Throwable t) {
            getLogger().log(1, "Cannot start ActiveMQ connection", t);
        }
    }

    @Override
    protected void onClose() {
        for (Map.Entry<String, Bridge> entry : this.bridges.entrySet()) {
            entry.getValue().close();
        }
        try {
            this.session.close();
        } catch (Throwable ignored) { }
        try {
            this.connection.close();
        } catch (Throwable ignored) { }
    }

    @Override
    protected void onSubscribe(@NotNull String... channels) {
        for (@NotNull String channel : channels) {
            if (!this.bridges.containsKey(channel)) {
                try {
                    this.bridges.put(channel, new Bridge(channel));
                } catch (Throwable t) {
                    getLogger().log(2, "Cannot subscribe to channel '" + channel + "'", t);
                }
            }
        }
    }

    @Override
    protected void onUnsubscribe(@NotNull String... channels) {
        for (@NotNull String channel : channels) {
            final Bridge bridge = this.bridges.get(channel);
            if (bridge != null) {
                bridge.close();
            }
        }
    }

    @Override
    protected void onSend(@NotNull String channel, byte[] data) throws IOException {
        final Bridge bridge = this.bridges.get(channel);
        if (bridge == null) {
            throw new IllegalArgumentException("The current broker is not subscribed to channel '" + channel + "'");
        }
        try {
            bridge.send(data);
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    public class Bridge implements MessageListener {

        private final String channel;
        private final MessageProducer producer;
        private final MessageConsumer consumer;

        public Bridge(@NotNull String channel) throws JMSException {
            this.channel = channel;

            final Topic topic = ActiveMQBroker.this.session.createTopic(channel);
            this.producer = ActiveMQBroker.this.session.createProducer(topic);
            this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            this.consumer = ActiveMQBroker.this.session.createConsumer(topic);
            this.consumer.setMessageListener(this);
        }

        @Override
        public void onMessage(Message message) {
            try {
                if (message instanceof BytesMessage) {
                    final BytesMessage bytesMessage = (BytesMessage) message;
                    final byte[] data = new byte[(int) bytesMessage.getBodyLength()];
                    bytesMessage.readBytes(data);

                    ActiveMQBroker.this.receive(this.channel, data);
                }
            } catch (JMSException | IOException e) {
                getLogger().log(2, "Cannot process received message from channel '" + this.channel + "'", e);
            }
        }

        @NotNull
        public String getChannel() {
            return channel;
        }

        @NotNull
        public MessageProducer getProducer() {
            return producer;
        }

        @NotNull
        public MessageConsumer getConsumer() {
            return consumer;
        }

        public void send(byte[] data) throws JMSException {
            final BytesMessage message = ActiveMQBroker.this.session.createBytesMessage();
            message.writeBytes(data);
            this.producer.send(message);
        }

        public void close() {
            try {
                this.producer.close();
            } catch (Throwable ignored) { }
            try {
                this.consumer.close();
            } catch (Throwable ignored) { }
        }
    }
}

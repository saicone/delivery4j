package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.Broker;
import com.saicone.delivery4j.util.LogFilter;
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

/**
 * ActiveMQ broker implementation to send data using topic producers and consumers.
 *
 * @author Rubenicos
 */
public class ActiveMQBroker extends Broker {

    private final Connection connection;

    private Session session;

    private final Map<String, Bridge> bridges = new HashMap<>();

    /**
     * Create an activemq broker by providing connection factory.
     *
     * @param consumer the operation that accept the activemq connection factory.
     * @return         a newly generated activemq broker.
     * @throws JMSException if any error occurs while creating the connection.
     */
    @NotNull
    public static ActiveMQBroker of(@NotNull Consumer<ActiveMQConnectionFactory> consumer) throws JMSException {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        consumer.accept(factory);
        return new ActiveMQBroker(factory.createConnection());
    }

    /**
     * Create an activemq broker by providing a connection.
     *
     * @param connection the connection used by the broker.
     */
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
            getLogger().log(LogFilter.ERROR, "Cannot start ActiveMQ connection", t);
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
                    getLogger().log(LogFilter.WARNING, "Cannot subscribe to channel '" + channel + "'", t);
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
    public void send(@NotNull String channel, byte[] data) throws IOException {
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

    /**
     * Bridge class to save message producer and consumer for specific topic.
     */
    public class Bridge implements MessageListener {

        private final String channel;
        private final MessageProducer producer;
        private final MessageConsumer consumer;

        /**
         * Constructs a bridge with the provided channel.
         *
         * @param channel the channel that will be used to create a topic.
         * @throws JMSException if any error occurs while creating activemq objects.
         */
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
                getLogger().log(LogFilter.WARNING, "Cannot process received message from channel '" + this.channel + "'", e);
            }
        }

        /**
         * Get the current subscribed channel.
         *
         * @return the topic as channel name.
         */
        @NotNull
        public String getChannel() {
            return channel;
        }

        /**
         * Get the current producer used to send messages to topic.
         *
         * @return a message producer.
         */
        @NotNull
        public MessageProducer getProducer() {
            return producer;
        }

        /**
         * Get the current consumer used to listed messages from topic.
         *
         * @return a message consumer.
         */
        @NotNull
        public MessageConsumer getConsumer() {
            return consumer;
        }

        /**
         * Send data to producer.
         *
         * @param data the data that will be wrapped as bytes message.
         * @throws JMSException if any error occurs while sending the data.
         */
        public void send(byte[] data) throws JMSException {
            final BytesMessage message = ActiveMQBroker.this.session.createBytesMessage();
            message.writeBytes(data);
            this.producer.send(message);
        }

        /**
         * Close the current activemq connection.
         */
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

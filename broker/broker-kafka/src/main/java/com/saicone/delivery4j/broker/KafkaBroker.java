package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.Broker;
import com.saicone.delivery4j.util.LogFilter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Kafka broker implementation to publish and consumer using empty-keys records.<br>
 * Take in count that any producer and consumer used in this instance should be
 * compatible with byte array values.
 *
 * @author Rubenicos
 *
 * @param <K> the key object type.
 */
public class KafkaBroker<K> extends Broker {

    private final KafkaProducer<K, byte[]> producer;
    private final KafkaConsumer<K, byte[]> consumer;

    private Integer partition = null;
    private Iterable<Header> headers = null;
    private K key = null;
    private Duration timeout = Duration.ofSeconds(5);
    private long sleepTime = 8;
    private TimeUnit sleepUnit = TimeUnit.SECONDS;

    private Object listenTask;
    private boolean reconnected = false;

    /**
     * Create a kafka broker with provided parameters.<br>
     * This method by default use `void` keys to provide a full empty-key interaction.
     *
     * @param bootstrapServers the boostrap server config.
     * @param groupId          the consumer group ID.
     * @return                 a newly generated kafka broker.
     */
    @NotNull
    public static KafkaBroker<Void> of(@NotNull String bootstrapServers, @NotNull String groupId) {
        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        return new KafkaBroker<>(producerProperties, consumerProperties);
    }

    /**
     * Constructs a kafka broker using provided properties for producer and consumer.
     *
     * @param producerProperties the properties used to construct a producer.
     * @param consumerProperties the properties used to construct a consumer.
     */
    public KafkaBroker(@NotNull Properties producerProperties, @NotNull Properties consumerProperties) {
        this(new KafkaProducer<>(producerProperties), new KafkaConsumer<>(consumerProperties));
    }

    /**
     * Constructs a kafka broker using provided producer and consumer.
     *
     * @param producer the producer to send messages.
     * @param consumer the consumer that accept messages.
     */
    public KafkaBroker(@NotNull KafkaProducer<K, byte[]> producer, @NotNull KafkaConsumer<K, byte[]> consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }

    @Override
    protected void onStart() {
        try {
            if (!getSubscribedChannels().isEmpty()) {
                this.consumer.subscribe(getSubscribedChannels());
            }
            setEnabled(true);
        } catch (Throwable t) {
            getLogger().log(LogFilter.ERROR, "Cannot subscribe Kafka consumer to channels", t);
        }

        if (isEnabled()) {
            this.listenTask = getExecutor().execute(this::listen);
        }
    }

    @Override
    protected void onClose() {
        try {
            this.producer.close();
        } catch (Throwable ignore) { }
        try {
            this.consumer.close();
        } catch (Throwable ignore) { }
        if (this.listenTask != null) {
            getExecutor().cancel(this.listenTask);
            this.listenTask = null;
        }
    }

    @Override
    protected void onSubscribe(@NotNull String... channels) {
        try {
            this.consumer.unsubscribe();
        } catch (Throwable ignored) { }
        if (!getSubscribedChannels().isEmpty()) {
            this.consumer.subscribe(getSubscribedChannels());
        }
    }

    @Override
    protected void onUnsubscribe(@NotNull String... channels) {
        try {
            this.consumer.unsubscribe();
        } catch (Throwable ignored) { }
        if (!getSubscribedChannels().isEmpty()) {
            this.consumer.subscribe(getSubscribedChannels());
        }
    }

    @Override
    public void send(@NotNull String channel, byte[] data) throws IOException {
        send(new ProducerRecord<>(channel, this.partition, this.key, data, this.headers));
    }

    /**
     * Send a producer record into current producer.
     *
     * @param record the record to send.
     * @throws IOException if send fails.
     */
    public void send(@NotNull ProducerRecord<K, byte[]> record) throws IOException {
        try {
            this.producer.send(record);
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    /**
     * Set the default partition used on every producer record created by this instance.
     *
     * @param partition the partition to which the records should be sent.
     */
    public void setPartition(@Nullable Integer partition) {
        this.partition = partition;
    }

    /**
     * Set the default headers used on every producer record created by this instance.
     *
     * @param headers the headers that will be included in the records.
     */
    public void setHeaders(@Nullable Iterable<Header> headers) {
        this.headers = headers;
    }

    /**
     * Set the default key used on every producer record created by this instance.
     *
     * @param key the key that will be included in the records.
     */
    public void setKey(@Nullable K key) {
        this.key = key;
    }

    /**
     * Set the record poll timeout, this is the maximum communication delay between applications.<br>
     * By default, 5 seconds is used.
     *
     * @param time the maximum time to block.
     * @param unit the unit that {@code time} is expressed in.
     */
    public void setTimeout(long time, @NotNull TimeUnit unit) {
        this.timeout = Duration.ZERO.plusNanos(unit.toNanos(time));
    }

    /**
     * Set the reconnection interval that will be used on this kafka broker instance.<br>
     * By default, 8 seconds is used.
     *
     * @param time the time to wait until reconnection is performed.
     * @param unit the unit that {@code time} is expressed in.
     */
    public void setReconnectionInterval(int time, @NotNull TimeUnit unit) {
        this.sleepTime = time;
        this.sleepUnit = unit;
    }

    /**
     * Get the current producer.
     *
     * @return a kafka producer.
     */
    @NotNull
    public KafkaProducer<K, byte[]> getProducer() {
        return producer;
    }

    private void listen() {
        try {
            while (isEnabled() && !Thread.interrupted()) {
                final ConsumerRecords<K, byte[]> records = this.consumer.poll(this.timeout);
                if (this.reconnected) {
                    this.reconnected = false;
                    getLogger().log(LogFilter.INFO, "Kafka connection is alive again");
                }
                for (ConsumerRecord<K, byte[]> record : records) {
                    final String channel = record.topic();
                    final byte[] data = record.value();
                    try {
                        receive(channel, data);
                    } catch (Throwable t) {
                        getLogger().log(LogFilter.WARNING, "Cannot process received message from channel '" + channel + "'", t);
                    }
                }
            }
        } catch (Throwable t) {
            if (!isEnabled()) {
                return;
            }
            this.reconnected = true;
            getLogger().log(LogFilter.WARNING, () -> "Kafka connection dropped, automatic reconnection every " + this.sleepTime + " " + this.sleepUnit.name().toLowerCase() + "...", t);
            try {
                Thread.sleep(this.sleepUnit.toMillis(this.sleepTime));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            listen();
        }
    }
}

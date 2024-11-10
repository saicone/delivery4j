package com.saicone.delivery4j;

import com.saicone.delivery4j.broker.TestBroker;
import com.saicone.delivery4j.impl.TestMessenger;
import com.saicone.delivery4j.util.Encryptor;
import org.junit.jupiter.api.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class MessengerTest {

    private static final String CHANNEL = "test:channel";
    private static final String MESSAGE = "Hello World";
    private static final String ALGORITHM = "AES";

    @Test
    public void testDelivery() {
        final TestMessenger messenger = new TestMessenger();
        messenger.start(new TestBroker());

        final String[] result = new String[2];
        messenger.subscribe(CHANNEL).consume((channel, lines) -> {
            result[0] = channel;
            result[1] = lines[0];
        });

        messenger.send(CHANNEL, MESSAGE);

        assertEquals(CHANNEL, result[0]);
        assertEquals(MESSAGE, result[1]);
    }

    @Test
    public void testCache() {
        final TestMessenger messenger = new TestMessenger();
        messenger.start();

        final String[] result = new String[2];
        messenger.subscribe(CHANNEL).consume((channel, lines) -> {
            result[0] = channel;
            result[1] = lines[0];
        }).cache(true);

        messenger.send(CHANNEL, MESSAGE);

        assertNull(result[0]);
        assertNull(result[1]);
    }

    @Test
    public void testEncryption() throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException {
        final TestMessenger messenger = new TestMessenger();
        messenger.start(new TestBroker());

        KeyGenerator keyGen = KeyGenerator.getInstance(ALGORITHM);
        keyGen.init(128);
        final SecretKey key = keyGen.generateKey();

        final String[] result = new String[2];
        messenger.subscribe(CHANNEL).consume((channel, lines) -> {
            result[0] = channel;
            result[1] = lines[0];
        }).encryptor(Encryptor.of(ALGORITHM, key));

        messenger.send(CHANNEL, MESSAGE);

        assertEquals(CHANNEL, result[0]);
        assertEquals(MESSAGE, result[1]);
    }
}

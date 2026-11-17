package com.saicone.delivery4j;

import com.saicone.delivery4j.util.Encryptor;
import org.junit.jupiter.api.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EncryptorTest {

    private static final String ALGORITHM = "AES";
    private static final int THREADS = 8;
    private static final int TASKS = 500;

    @Test
    public void testConcurrentEncryption() throws Exception {
        final KeyGenerator keyGen = KeyGenerator.getInstance(ALGORITHM);
        keyGen.init(128);
        final SecretKey key = keyGen.generateKey();

        final Encryptor encryptor = Encryptor.of(ALGORITHM, key);

        final ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        try {
            final List<Callable<Void>> tasks = new ArrayList<>();
            for (int i = 0; i < TASKS; i++) {
                final String message = "concurrent message " + i;
                tasks.add(() -> {
                    final byte[] encrypted = encryptor.encrypt(message.getBytes(StandardCharsets.UTF_8));
                    final String decrypted = new String(encryptor.decrypt(encrypted), StandardCharsets.UTF_8);
                    assertEquals(message, decrypted);
                    return null;
                });
            }

            for (Future<Void> future : executor.invokeAll(tasks)) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }
    }
}

package com.saicone.delivery4j;

import com.saicone.delivery4j.util.ByteCodec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ByteCodecTest {

    private static final String TEXT = "Hello World! This is a message that should be encoded";

    @Test
    public void test() {
        String encoded;
        String decoded;

        encoded = ByteCodec.BASE64.encodeFromString(TEXT);
        decoded = ByteCodec.BASE64.decodeToString(encoded);
        assertEquals(TEXT, decoded);

        encoded = ByteCodec.Z85.encodeFromString(TEXT);
        decoded = ByteCodec.Z85.decodeToString(encoded);
        assertEquals(TEXT, decoded);
    }
}

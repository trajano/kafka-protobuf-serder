package net.trajano.kafka.serde;

import com.example.tutorial.Msg;
import org.junit.jupiter.api.Test;

import java.util.Map;


class ProtobufSerDeTest {

    @Test
    void deserialize() {
    }

    @Test
    void testDeserialize() {
    }

    @Test
    void serialize() {
    }

    @Test
    void close() {
        final ProtobufSerDe<Msg> msgProtobufSerDe = new ProtobufSerDe<>();
        msgProtobufSerDe.close();
    }

    @Test
    void configureKey() {

        final ProtobufSerDe<Msg> msgProtobufSerDe = new ProtobufSerDe<>();
        msgProtobufSerDe.configure(Map.of(), true);
        msgProtobufSerDe.close();
    }

    @Test
    void configureValue() {

        final ProtobufSerDe<Msg> msgProtobufSerDe = new ProtobufSerDe<>();
        msgProtobufSerDe.configure(Map.of(), false);
        msgProtobufSerDe.close();
    }

    @Test
    void testSerialize() {
    }
}
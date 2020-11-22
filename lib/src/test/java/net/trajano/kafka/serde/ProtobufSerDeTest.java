package net.trajano.kafka.serde;

import com.example.tutorial.Msg;
import com.example.tutorial.SecondMsg;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class ProtobufSerDeTest {

    @Test
    void configureKey() {

        final ProtobufSerDe<Msg> msgProtobufSerDe = new ProtobufSerDe<>();
        msgProtobufSerDe.configure(Map.of(
                ProtobufSerDe.KEY_DEFAULT_TYPE, Msg.class.getName(),
                ProtobufSerDe.KEY_USE_TYPE_INFO, false
        ), true);
        final Msg msg = Msg.newBuilder()
                .setFoo("blah")
                .build();
        byte[] bytes = msgProtobufSerDe.serialize("", msg);
        final Msg deserialized = msgProtobufSerDe.deserialize("", bytes);
        assertThat(deserialized).isEqualTo(msg);

        msgProtobufSerDe.close();
    }

    @Test
    void deserializeValue() {

        final ProtobufSerDe<Msg> msgProtobufSerDe = new ProtobufSerDe<>();
        msgProtobufSerDe.configure(Map.of(
                ProtobufSerDe.VALUE_DEFAULT_TYPE, Msg.class.getName()
        ), false);
        final Msg msg = Msg.newBuilder()
                .setFoo("blah")
                .build();
        byte[] bytes = msgProtobufSerDe.serialize("", msg);
        final Msg deserialized = msgProtobufSerDe.deserialize("", bytes);
        assertThat(deserialized).isEqualTo(msg);

        msgProtobufSerDe.close();
    }

    @Test
    void deserializeValueWrongType() {

        final ProtobufSerDe<Msg> msgProtobufSerDe = new ProtobufSerDe<>();
        msgProtobufSerDe.configure(Map.of(
                ProtobufSerDe.VALUE_DEFAULT_TYPE, Msg.class.getName()
        ), false);

        final ProtobufSerDe<SecondMsg> secondMsgProtobufSerDe = new ProtobufSerDe<>();
        secondMsgProtobufSerDe.configure(Map.of(
                ProtobufSerDe.VALUE_DEFAULT_TYPE, SecondMsg.class.getName()
        ), false);

        final SecondMsg msg = SecondMsg.newBuilder()
                .setBlah(42)
                .build();
        byte[] bytes = secondMsgProtobufSerDe.serialize("", msg);
        Msg deserialized = msgProtobufSerDe.deserialize("", bytes);
        assertThat(deserialized.hasBlah()).isFalse();

    }

    @Test
    void deserializeWithTypeInfo() {

        final ProtobufSerDe<SecondMsg> secondMsgProtobufSerDe = new ProtobufSerDe<>();
        secondMsgProtobufSerDe.configure(Map.of(
                ProtobufSerDe.VALUE_DEFAULT_TYPE, SecondMsg.class.getName(),
                ProtobufSerDe.VALUE_USE_TYPE_INFO, true
        ), false);

        final SecondMsg msg = SecondMsg.newBuilder()
                .setBlah(42)
                .build();
        byte[] bytes = secondMsgProtobufSerDe.serialize("", msg);
        final Headers headers = mock(Headers.class);

        final Header header = mock(Header.class);
        when(header.value()).thenReturn(SecondMsg.class.getName().getBytes(StandardCharsets.US_ASCII));

        when(headers.lastHeader(ProtobufSerDe.VALUE_TYPE)).thenReturn(header);

        final SecondMsg deserialized = secondMsgProtobufSerDe.deserialize("", headers, bytes);
        assertThat(deserialized).isEqualTo(msg);

    }

    @Test
    void deserializeWithTypeInfoMissing() {

        final ProtobufSerDe<SecondMsg> secondMsgProtobufSerDe = new ProtobufSerDe<>();
        secondMsgProtobufSerDe.configure(Map.of(
                ProtobufSerDe.VALUE_DEFAULT_TYPE, SecondMsg.class.getName(),
                ProtobufSerDe.VALUE_USE_TYPE_INFO, true
        ), false);

        final SecondMsg msg = SecondMsg.newBuilder()
                .setBlah(42)
                .build();
        byte[] bytes = secondMsgProtobufSerDe.serialize("", msg);
        final Headers headers = mock(Headers.class);

        final Header header = mock(Header.class);
        when(header.value()).thenReturn(SecondMsg.class.getName().getBytes(StandardCharsets.US_ASCII));

        final SecondMsg deserialized = secondMsgProtobufSerDe.deserialize("", headers, bytes);
        assertThat(deserialized).isEqualTo(msg);

    }

    @Test
    void deserializeKeyWithTypeInfoMissing() {

        final ProtobufSerDe<SecondMsg> secondMsgProtobufSerDe = new ProtobufSerDe<>();
        secondMsgProtobufSerDe.configure(Map.of(
                ProtobufSerDe.KEY_DEFAULT_TYPE, SecondMsg.class.getName(),
                ProtobufSerDe.KEY_USE_TYPE_INFO, true,
                ProtobufSerDe.VALUE_DEFAULT_TYPE, SecondMsg.class.getName(),
                ProtobufSerDe.VALUE_USE_TYPE_INFO, true
        ), true);

        final SecondMsg msg = SecondMsg.newBuilder()
                .setBlah(42)
                .build();
        byte[] bytes = secondMsgProtobufSerDe.serialize("", msg);
        final Headers headers = mock(Headers.class);

        final Header header = mock(Header.class);
        when(header.value()).thenReturn(SecondMsg.class.getName().getBytes(StandardCharsets.US_ASCII));

        final SecondMsg deserialized = secondMsgProtobufSerDe.deserialize("", headers, bytes);
        assertThat(deserialized).isEqualTo(msg);

    }

    @Test
    void configureClassNotFound() {

        final ProtobufSerDe<Msg> msgProtobufSerDe = new ProtobufSerDe<>();
        Map<String, String> configs = Map.of(
                ProtobufSerDe.VALUE_DEFAULT_TYPE, "foo"
        );
        assertThatThrownBy(() ->
                msgProtobufSerDe.configure(configs, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void configureClassNotMessage() {

        final ProtobufSerDe<Msg> msgProtobufSerDe = new ProtobufSerDe<>();
        Map<String, String> configs = Map.of(
                ProtobufSerDe.VALUE_DEFAULT_TYPE, String.class.getName()
        );
        assertThatThrownBy(() ->
                msgProtobufSerDe.configure(configs, false))
                .isInstanceOf(IllegalArgumentException.class);
    }


    @Test
    void deserializeValueMissingDefaultType() {

        final ProtobufSerDe<Msg> msgProtobufSerDe = new ProtobufSerDe<>();
        msgProtobufSerDe.configure(Map.of(
                ProtobufSerDe.KEY_DEFAULT_TYPE, Msg.class.getName()
        ), false);
        final Msg msg = Msg.newBuilder()
                .setFoo("blah")
                .build();
        byte[] bytes = msgProtobufSerDe.serialize("", msg);
        assertThatThrownBy(() -> msgProtobufSerDe.deserialize("", bytes))
                .isInstanceOf(IllegalStateException.class);
    }

}
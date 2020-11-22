package net.trajano.kafka.serde;

import com.google.protobuf.Message;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class ProtobufSerDe<T extends Message> implements Serializer<T>, Deserializer<T> {
    public static final String KEY_DEFAULT_TYPE;
    public static final String KEY_USE_TYPE_INFO;
    public static final String KEY_TYPE;

    public static final String VALUE_DEFAULT_TYPE;
    public static final String VALUE_USE_TYPE_INFO;
    public static final String VALUE_TYPE;

    private static final String PREFIX;

    static {

        PREFIX = "protobuf.serde.";

        KEY_DEFAULT_TYPE = PREFIX + "key.default.type";
        VALUE_DEFAULT_TYPE = PREFIX + "value.default.type";

        KEY_USE_TYPE_INFO = PREFIX + "key.use_type_info";
        VALUE_USE_TYPE_INFO = PREFIX + "value.use_type_info";

        KEY_TYPE = PREFIX + "key.type";
        VALUE_TYPE = PREFIX + "value.type";


    }

    private final Map<String, Method> parseFromMethodMap = new ConcurrentHashMap<>();
    private Method defaultParseFromMethod;
    private boolean isKey;
    private boolean useTypeInfo;

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(final String topic, final byte[] data) {
        Objects.requireNonNull(data);
        if (defaultParseFromMethod == null) {
            throw new IllegalStateException("No default parseFrom method and type info is missing");
        }
        try {
            return (T) defaultParseFromMethod.invoke(null, (Object) data);
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(final String topic, final Headers headers, final byte[] data) {
        Objects.requireNonNull(headers);
        Objects.requireNonNull(data);

        final Method parseFromMethod;
        if (!useTypeInfo) {
            parseFromMethod = defaultParseFromMethod;
        } else if (isKey) {
            parseFromMethod = parseMethodFromHeaderIfAvailable(headers, KEY_TYPE);
        } else {
            parseFromMethod = parseMethodFromHeaderIfAvailable(headers, VALUE_TYPE);
        }
        try {
            return (T) parseFromMethod.invoke(null, (Object) data);
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }

    }

    private Method parseMethodFromHeaderIfAvailable(final Headers headers, final String valueType2) {
        Method parseFromMethod;
        final Iterator<Header> it = headers.headers(valueType2).iterator();
        if (it.hasNext()) {
            final Header firstKeyTypeHeader = it.next();
            final String valueType = new String(firstKeyTypeHeader.value()).intern();
            parseFromMethod = parseFromMethodMap.computeIfAbsent(valueType, ProtobufSerDe::parseFromMethodFromClassName);
        } else {
            parseFromMethod = defaultParseFromMethod;
        }
        return parseFromMethod;
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return new byte[0];
    }

    @Override
    public void close() {

        // no op

    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {

        this.isKey = isKey;

        if (isKey) {
            if (configs.containsKey(KEY_DEFAULT_TYPE)) {
                defaultParseFromMethod = parseFromMethodFromClass(classForName((String) configs.get(KEY_DEFAULT_TYPE)));
            }
            if (configs.containsKey(KEY_USE_TYPE_INFO)) {
                useTypeInfo = Boolean.parseBoolean(String.valueOf(configs.get(KEY_USE_TYPE_INFO)));
            }
        } else {
            if (configs.containsKey(VALUE_DEFAULT_TYPE)) {
                defaultParseFromMethod = parseFromMethodFromClass(classForName((String) configs.get(VALUE_DEFAULT_TYPE)));
            }
            if (configs.containsKey(VALUE_USE_TYPE_INFO)) {
                useTypeInfo = Boolean.parseBoolean(String.valueOf(configs.get(VALUE_USE_TYPE_INFO)));
            }
        }

    }

    private static Method parseFromMethodFromClassName(final String className) {
        return parseFromMethodFromClass(classForName(className));
    }

    private static Class<?> classForName(final String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(String.format("class %s not found", className));
        }

    }

    private static Method parseFromMethodFromClass(final Class<?> clazz) {

        if (!Message.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format("%s is not assignable to %s", clazz, Message.class));
        }
        @SuppressWarnings("unchecked") final Class<? extends Message> messageClass = (Class<? extends Message>) clazz;
        try {
            final Method parseFrom = messageClass.getMethod("parseFrom", byte[].class);
            if (!Modifier.isStatic(parseFrom.getModifiers())) {
                throw new IllegalStateException(String.format("parseFrom method in %s is not static", messageClass));
            }
            return parseFrom;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(String.format("Missing expected parseFrom method in %s", messageClass), e);
        }

    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        Objects.requireNonNull(data);
        return data.toByteArray();
    }
}

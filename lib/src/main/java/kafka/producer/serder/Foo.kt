package kafka.producer.serder

import com.google.protobuf.Message
import org.apache.kafka.common.serialization.Serializer

class Foo<T : Message?> : Serializer<T> {
    private fun foo() {
        val x: Class<*> = ByteArray::class.java
    }

    override fun serialize(topic: String, data: T): ByteArray {
        return ByteArray(0)
    }
}
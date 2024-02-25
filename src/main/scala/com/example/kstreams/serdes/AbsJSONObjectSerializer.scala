package com.example.kstreams.serdes

import org.apache.kafka.common.serialization.Serializer
import org.json.JSONObject

import java.nio.charset.StandardCharsets


abstract class AbsJSONObjectSerializer[T] extends Serializer[T] with ObjectTransformer[T, JSONObject] {
  override def serialize(topic: String, data: T): Array[Byte] = {
    transform(data).toString().getBytes(StandardCharsets.UTF_8)
  }
}

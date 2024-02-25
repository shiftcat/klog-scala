package com.example.kstreams.serdes

import com.google.common.base.Throwables
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer
import org.json.JSONObject
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import scala.util.{Failure, Success, Try}

abstract class AbsJSONObjectDeserializer[T] extends Deserializer[T] with ObjectTransformer [JSONObject, T] {

  private val logger = LoggerFactory.getLogger(getClass)

  override implicit def deserialize(topic: String, data: Array[Byte]): T = {
    val t = Try(new JSONObject(new String(data, StandardCharsets.UTF_8)))
    val json = t match {
      case Success(j) => j
      case Failure(e) => logger.error(Throwables.getStackTraceAsString(e)); new JSONObject()
    }
    transform(json)
  }


  override def deserialize(topic: String, headers: Headers, data: Array[Byte]): T = {
    deserialize(topic, data)
  }

}

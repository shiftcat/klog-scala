package com.example.kstreams.serdes

import com.google.common.base.Throwables
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.LoggerFactory

import java.io.ByteArrayInputStream
import scala.util.{Failure, Success, Try, Using}

abstract class AbsGenericRecordDeserializer[T] extends Deserializer[T] with ObjectTransformer[GenericRecord, T] {

  private val logger = LoggerFactory.getLogger(getClass)

  def schema(data: Array[Byte]): Schema

  def schema(headers: Headers, data: Array[Byte]): Schema


  private def deserializer(schem: Schema, data: Array[Byte]): T = {
    val ot = Using(new ByteArrayInputStream(data)) { is =>
      val it = Try {
        val decoder = DecoderFactory.get.jsonDecoder(schem, is)
        val reader = new GenericDatumReader[GenericRecord](schem)
        reader.read(null, decoder)
      }
      it match {
        case Success(r) => Some(r)
        case Failure(e) => logger.error(Throwables.getStackTraceAsString(e)); None
      }
    }

    val record = ot match {
      case Success(r) => r.getOrElse(new GenericData.Record(schem))
      case Failure(e) => logger.error(Throwables.getStackTraceAsString(e)); new GenericData.Record(schem)
    }
    transform(record)
  }


  override def deserialize(topic: String, data: Array[Byte]): T = {
    val schem = schema(data)
    deserializer(schem, data)
  }


  override def deserialize(topic: String, headers: Headers, data: Array[Byte]): T = {
    val schem = schema(headers, data)
    deserializer(schem, data)
  }

}

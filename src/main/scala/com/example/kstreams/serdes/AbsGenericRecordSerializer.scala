package com.example.kstreams.serdes

import com.google.common.base.Throwables
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.LoggerFactory

import java.io.ByteArrayOutputStream
import scala.util.{Failure, Success, Try, Using}

abstract class AbsGenericRecordSerializer[T] extends Serializer[T] with ObjectTransformer[T, GenericRecord] {

  private val logger = LoggerFactory.getLogger(getClass)

  override def serialize(topic: String, data: T): Array[Byte] = {
    val record = transform(data)
    val ot = Using(new ByteArrayOutputStream()) { os =>
      val it = Try {
        val encoder = EncoderFactory.get.jsonEncoder(record.getSchema, os)
        val writer = new GenericDatumWriter[GenericRecord](record.getSchema)
        writer.write(record, encoder)
        encoder.flush()
        os.toByteArray
      }
      it match {
        case Success(b) => Some(b)
        case Failure(e) => logger.error(Throwables.getStackTraceAsString(e)); None
      }
    }

    ot match {
      case Success(b) => b.getOrElse(Array.empty[Byte])
      case Failure(e) => logger.error(Throwables.getStackTraceAsString(e)); Array.empty[Byte]
    }
  }

}

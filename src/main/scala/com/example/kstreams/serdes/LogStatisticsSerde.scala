package com.example.kstreams.serdes

import com.example.kstreams.code.StatType
import com.example.kstreams.model.dto.LogStatistics
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.json.JSONObject

import java.nio.charset.StandardCharsets

class LogStatisticsSerde extends Serde[LogStatistics] {

  override def serializer(): Serializer[LogStatistics] = {
    new LogStatisticsSerializer()
  }

  override def deserializer(): Deserializer[LogStatistics] = {
    new LogStatisticsDeserializer()
  }


  private class LogStatisticsSerializer extends AbsGenericRecordSerializer[LogStatistics] {
    override def transform(data: LogStatistics): GenericRecord = {
      data.toGenericRecord
    }
  }

  private class LogStatisticsDeserializer extends AbsGenericRecordDeserializer[LogStatistics] {
    override def transform(data: GenericRecord): LogStatistics = {
      LogStatistics(data)
    }

    // data의 statType 필드의 값으로 schema 리턴
    override def schema(data: Array[Byte]): Schema = {
      val s = new String(data, StandardCharsets.UTF_8)
      val json = new JSONObject(s)
      val statType = StatType(json.getString("statType")).orNull
      LogStatistics.schema(statType)
    }

    // Header의 Stat-type 값으로 schema 리턴
    override def schema(headers: Headers, data: Array[Byte]): Schema = {
      val t = headers.toArray.find(_.key() == "Stat-type").map(_.value()).map(new String(_, StandardCharsets.UTF_8))
      t match {
        case Some(statType) =>
          val st = StatType(statType).orNull
          LogStatistics.schema(st)
        case _ => schema(data)
      }
    }
  }

}

package com.example.kstreams.serdes

import com.example.kstreams.code.LogType._
import com.example.kstreams.model.dto.{DummyLog, EventLog, RequestEventLog, ResponseEventLog}
import com.example.kstreams.utils.JsonUtil
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.json.JSONObject

import scala.language.implicitConversions


class EventLogJsonSerde extends Serde[EventLog] {

  override def serializer(): Serializer[EventLog] = {
    new EventLogSerializer()
  }

  override def deserializer(): Deserializer[EventLog] = {
    new EventLogDeserializer()
  }

  private class EventLogSerializer extends AbsJSONObjectSerializer[EventLog] {
    override def transform(data: EventLog): JSONObject = {
      data.toJSON
    }
  }

  private class EventLogDeserializer extends AbsJSONObjectDeserializer[EventLog] {

    override def transform(json: JSONObject): EventLog = {
      val jsonUtil = JsonUtil(json)
      val logType: Option[LogType] = jsonUtil.enumValue("log_type")
       logType match {
        case Some(REQ) => RequestEventLog(jsonUtil)
        case Some(RES) => ResponseEventLog(jsonUtil)
        case None => DummyLog()
      }
    }

  }

}

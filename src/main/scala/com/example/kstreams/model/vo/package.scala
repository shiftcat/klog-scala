package com.example.kstreams.model

import com.example.kstreams.constant.dateTimeFormatter
import com.example.kstreams.utils.JsonUtil
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{Schema, SchemaBuilder}
import org.json.JSONObject

import java.time.Instant
import scala.language.implicitConversions


package object vo {

  @SerialVersionUID(1L)
  case class Caller(channel: String, channelIp: String) {
    def toJSON: JSONObject = {
      new JSONObject()
        .put("name", channel)
        .put("ip", channelIp)
    }
  }

  object Caller {
    def apply(obj: JsonUtil): Caller = {
      Caller(
        channel = obj.getString("channel").orNull,
        channelIp = obj.getString("channelIp").orNull
      )
    }

    implicit def json2Caller(json: JSONObject): Caller = {
      Caller(json)
    }

    implicit def extractor(o: Option[JSONObject]): Caller = {
      o.map(Caller(_)).orNull
    }
  }


  @SerialVersionUID(1L)
  case class Host(name: String, ip: String) {
    def toJSON: JSONObject = {
      new JSONObject()
        .put("name", name)
        .put("ip", ip)
    }
  }

  object Host {
    def apply(obj: JsonUtil): Host = {
      Host(
        name = obj.getString("name").orNull,
        ip = obj.getString("ip").orNull
      )
    }

    implicit def json2Host(json: JSONObject): Host = {
      Host(json)
    }

    implicit def extractor(o: Option[JSONObject]): Host = {
      o.map(Host(_)).orNull
    }
  }


  @SerialVersionUID(1L)
  case class User(id: String, ip: String, agent: String) {
    def toJSON: JSONObject = {
      new JSONObject()
        .put("id", id)
        .put("ip", ip)
        .put("agent", agent)
    }
  }

  object User {
    def apply(obj: JsonUtil): User = {
      User(
        id = obj.getString("id").orNull,
        ip = obj.getString("ip").orNull,
        agent = obj.getString("agent").orNull
      )
    }

    implicit def json2User(json: JSONObject): User = {
      User(json)
    }

    implicit def extractor(o: Option[JSONObject]): User = {
      o.map(User(_)).orNull
    }

    def builder(): Builder = {
      new Builder()
    }

    class Builder {
      private var id: String = _
      private var ip: String = _
      private var agent: String = _

      def id(id: String): Builder = {
        this.id = id
        this
      }

      def ip(ip: String): Builder = {
        this.ip = ip
        this
      }

      def agent(agent: String): Builder = {
        this.agent = agent
        this
      }

      def build(): User = {
        User(this.id, this.ip, this.agent)
      }
    }
  }


  @SerialVersionUID(1L)
  case class Response(`type`: String, status: Int, desc: String, duration: Long) {
    def toJSON: JSONObject = {
      new JSONObject()
        .put("type", `type`)
        .put("status", status)
        .put("desc", desc)
        .put("duration", duration)
    }
  }

  object Response {
    def apply(obj: JsonUtil): Response = {
      Response(
        `type` = obj.getString("type").orNull,
        status = obj.getInt("status").getOrElse(-1),
        desc = obj.getString("desc").getOrElse(""),
        duration = obj.getLong("duration").getOrElse(-1)
      )
    }

    implicit def json2Response(json: JSONObject): Response = {
      Response(json)
    }

    implicit def extractor(o: Option[JSONObject]): Response = {
      o.map(Response(_)).orNull
    }
  }

  @SerialVersionUID(1L)
  case class Metadata(topic: String, partition: Int, offset: Long) {
    def toJSON: JSONObject = {
      new JSONObject()
        .put("topic", topic)
        .put("partition", partition)
        .put("offset", offset)
    }
  }

  object Metadata {
    def apply(json: JsonUtil): Metadata = {
      Metadata(
        topic = json.getString("topic").orNull,
        partition = json.getInt("partition").getOrElse(-1),
        offset = json.getLong("offset").getOrElse(-1)
      )
    }

    implicit def json2Metadata(json: JSONObject): Metadata = {
      Metadata(json)
    }

    implicit def extractor(o: Option[JSONObject]): Metadata = {
      o.map(Metadata(_)).orNull
    }
  }


  @SerialVersionUID(1L)
  case class StatWindow(start: Long, end: Long) {
    def toGenericRecord(): GenericRecord = {
      val record = new GenericData.Record(StatWindow.schema())
      record.put("start", start)
      record.put("end", end)
      record
    }

    override def toString: String = {
      s"${getClass.getSimpleName.replace("""$""", "")}(start=${dateTimeFormatter.format(Instant.ofEpochMilli(this.start))}, end=${dateTimeFormatter.format(Instant.ofEpochMilli(this.end))})"
    }
  }

  object StatWindow {
    def of(start: Long, end: Long): StatWindow = {
      StatWindow(start, end)
    }


    def schema(): Schema = {
      SchemaBuilder.record(StatWindow.getClass.getSimpleName.replace("""$""", ""))
        .fields()
        .requiredLong("start").requiredLong("end")
        .endRecord()
    }

    def from(record: GenericRecord) : StatWindow = {
      val start = record.get("start").ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]
      val end = record.get("end").ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]
      StatWindow(start, end)
    }
  }


  @SerialVersionUID(1L)
  case class ServiceOperation(service: String, operation: String) {
    def toGenericRecord: GenericRecord = {
      val record = new GenericData.Record(ServiceOperation.schema())
      record.put("service", service)
      record.put("operation", operation)
      record
    }
  }

  object ServiceOperation {
    def of(service: String, operation: String): ServiceOperation = {
      new ServiceOperation(service, operation)
    }

    def schema(): Schema = {
      SchemaBuilder.record(ServiceOperation.getClass.getSimpleName.replace("""$""", ""))
        .fields()
        .requiredString("service").requiredString("operation")
        .endRecord()
    }

    def from(record: GenericRecord): ServiceOperation = {
      val service = record.get("service").ensuring(_.isInstanceOf[String]).asInstanceOf[String]
      val operation = record.get("operation").ensuring(_.isInstanceOf[String]).asInstanceOf[String]
      ServiceOperation(service, operation)
    }
  }

}

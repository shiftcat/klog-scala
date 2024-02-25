package com.example.kstreams.model.dto

import com.example.kstreams.code
import com.example.kstreams.code.LogType
import com.example.kstreams.code.LogType._
import com.example.kstreams.model.vo._
import com.example.kstreams.utils.JsonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.builder.EqualsBuilder
import org.json.JSONObject
import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions


trait EventLog {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  val logType: LogType.Value
  val traceId: String
  val spanId: String
  val service: String
  val operation: String

  val caller: Caller
  val host: Host
  val destination: Host
  val user: User
  val unixTimestamp: Long

  var metadata: Metadata


  def toJSON: JSONObject = {
    val json = new JSONObject()
      .put("log_tye", logType.toString)
      .put("trace_id", traceId)
      .put("span_id", spanId)
      .put("service", service)
      .put("operation", operation)
      .put("caller", caller.toJSON)
      .put("host", host.toJSON)
      .put("destination", destination.toJSON)
      .put("user", user.toJSON)
      .put("event_dt", unixTimestamp)
    if (metadata != null) {
      json.put("metadata", metadata.toJSON)
    }
    json
  }


  def validate: Boolean = {
    if (StringUtils.isEmpty(traceId)) {
      logger.warn("TraceId value is empty.")
      return false
    }
    if (StringUtils.isEmpty(spanId)) {
      logger.warn("SpanId value is empty.")
      return false
    }
    if (logType == null) {
      logger.warn("LogType value is empty.")
      return false
    }
    if (StringUtils.isEmpty(service)) {
      logger.warn("Service value is empty.")
      return false
    }
    if (StringUtils.isEmpty(operation)) {
      logger.warn("Operation value is empty.")
      return false
    }
    if(caller == null) {
      logger.warn("Caller is null.")
      return false
    } else if(StringUtils.isEmpty(caller.channel)) {
      logger.warn("Channel value is empty.")
      return false
    }
    if (host == null) {
      logger.warn("Host is null.")
      return false
    }
    else if (StringUtils.isEmpty(host.name)) {
      logger.warn("Host name value is empty.")
      return false
    }
    if (unixTimestamp == -1) {
      logger.warn("Timestamp value is wrong.")
      return false
    }
    true
  }

  override def equals(obj: Any): Boolean = {
    if(!obj.isInstanceOf[EventLog]) return false
    val eventLog = obj.asInstanceOf[EventLog]
    if(this eq eventLog) return true
    new EqualsBuilder()
      .append(traceId, eventLog.traceId)
      .append(spanId, eventLog.spanId)
      .append(service, eventLog.service)
      .append(operation, eventLog.operation)
      .append(caller, eventLog.caller)
      .append(host, eventLog.host)
      .append(destination, eventLog.destination)
      .append(user, eventLog.user).isEquals
  }

}


final case class DummyLog(
  logType: code.LogType.Value,
  traceId: String,
  spanId: String,
  service: String,
  operation: String,
  caller: Caller,
  host: Host,
  destination: Host,
  user: User,
  unixTimestamp: Long,
  var metadata: Metadata
) extends EventLog {
  def this() = this(REQ, null, null, null, null, null, null, null, null, -1, null)
}

object DummyLog {
  def apply(): DummyLog = {
    new DummyLog()
  }
}


trait RequestLog extends EventLog {
}

trait ResponseLog extends EventLog {
  val response: Response
}

case class RequestEventLog(
  logType: LogType = REQ,
  traceId: String,
  spanId: String,
  service: String,
  operation: String,
  caller: Caller,
  host: Host,
  destination: Host,
  user: User,
  unixTimestamp: Long,
  var metadata: Metadata = null
) extends RequestLog {

  def this() = {
    this(REQ, null, null, null, null, null, null, null, null, -1)
  }

  override def toJSON: JSONObject = {
    super.toJSON
  }
}


case class ResponseEventLog(
   logType: LogType = RES,
   traceId: String,
   spanId: String,
   service: String,
   operation: String,
   caller: Caller,
   host: Host,
   destination: Host,
   user: User,
   response: Response,
   unixTimestamp: Long,
   var metadata: Metadata = null
) extends ResponseLog with RequestLog {

  def this() = {
    this(RES, null, null, null, null, null, null, null, null, null, -1)
  }

  override def toJSON: JSONObject = {
    super.toJSON.put("response", response.toJSON)
  }
}


object EventLog {
  implicit val stringExtractor: Option[String] => String = (o: Option[String]) => o.orNull
  implicit val longExtractor: Option[Long] => Long = (o: Option[Long]) => o.getOrElse(-1)
}


object RequestEventLog {

  import com.example.kstreams.model.dto.EventLog._

  def apply(jsonUtil: JsonUtil): RequestLog = {

    val logType: LogType = jsonUtil.enumValue("log_type").orNull

    val traceId = jsonUtil.extractString("trace_id")
    val spanId = jsonUtil.extractString("span_id")
    val service = jsonUtil.extractString("service")
    val operation = jsonUtil.extractString("operation")
    val eventDatetime: Long = jsonUtil.extractLong("event_dt")

    val caller: Caller = jsonUtil.extractValue("caller")
    val host: Host = jsonUtil.extractValue("host")
    val dest: Host = jsonUtil.extractValue("destination")
    val user: User = jsonUtil.extractValue("user")
    val metadata: Metadata = jsonUtil.extractValue("metadata")

    val event = new RequestEventLog(logType, traceId, spanId, service, operation, caller, host, dest, user, eventDatetime)
    event.metadata = metadata
    event
  }
}


object ResponseEventLog {

  import com.example.kstreams.model.dto.EventLog._

  def apply(jsonUtil: JsonUtil): ResponseEventLog = {
    val logType: LogType = jsonUtil.enumValue("log_type").orNull

    val traceId = jsonUtil.extractString("trace_id")
    val spanId = jsonUtil.extractString("span_id")
    val service = jsonUtil.extractString("service")
    val operation = jsonUtil.extractString("operation")
    val eventDatetime: Long = jsonUtil.extractLong("event_dt")

    val caller: Caller = jsonUtil.extractValue("caller")
    val host: Host = jsonUtil.extractValue("host")
    val dest: Host = jsonUtil.extractValue("destination")
    val user: User = jsonUtil.extractValue("user")
    val response: Response = jsonUtil.extractValue("response")
    val metadata: Metadata = jsonUtil.extractValue("metadata")
    val event = new ResponseEventLog(logType, traceId, spanId, service, operation, caller, host, dest, user, response, eventDatetime)
    event.metadata = metadata
    event
  }
}


package com.example.kstreams.model.dto


import com.example.kstreams.constant.dateTimeFormatter
import com.example.kstreams.model.dto.TraceLog._
import org.slf4j.{Logger, LoggerFactory}

import java.time.format.DateTimeFormatter
import java.time.Instant


case class TraceLog(
 traceId: String,
 var requestLogs: List[RequestEventLog] = List(),
 var responseLogs: List[ResponseEventLog] = List(),
 var checkCount: Int = 0,
 var checkDatetime: Long = 0L,
 var evictionDatetime: Long = 0L
) {


  def this(traceId: String, eventLog: EventLog) = {
    this(traceId)
    addLog(eventLog)
    this.evictionDatetime = System.currentTimeMillis + EVICTION_TIME_MS
    this.checkDatetime = System.currentTimeMillis + LOG_CHECK_START_MS
    logger.info(
      "Created tracelog: {}, eviction: {}, next check: {}",
      traceId, formatter.format(Instant.ofEpochMilli(evictionDatetime)),
      formatter.format(Instant.ofEpochMilli(checkDatetime)))
  }


  def addLog(eventLog: EventLog): Unit = {
    if (this.traceId == eventLog.traceId) {
      eventLog match {
        case req: RequestEventLog => requestLogs ::= req
        case res: ResponseEventLog => responseLogs ::= res
      }
    }
  }


  def isEmpty: Boolean =
    (this.requestLogs == null || requestLogs.isEmpty) && (this.responseLogs == null || this.responseLogs.isEmpty)


  def size: Int = {
    if (this.isEmpty) return 0
    this.requestLogs.size + this.responseLogs.size
  }


  def incrementCheckCount(): Unit = {
    this.checkCount += 1
  }

  def maxCheckCount: Boolean = MAX_CHECK_COUNT <= checkCount

  def afterEvictionDatetime(now: Long): Boolean = this.evictionDatetime <= now

  def beforeCheckDatetime(now: Long): Boolean = now < this.checkDatetime

  def nextCheckDatetime(): Unit = {
    this.checkDatetime = System.currentTimeMillis + LOG_CHECK_INTERVAL_MS
    logger.debug("TraceLog current check count: {}, next check datetime: {}",
      this.checkCount, formatter.format(Instant.ofEpochMilli(checkDatetime)))
  }


  private def findReqLog(resLog: ResponseEventLog): Option[RequestEventLog] = {
    this.requestLogs
      .find((l: RequestEventLog) => l == resLog)
  }


  def pairLogs(): List[(RequestEventLog, ResponseEventLog)] = {
    // 응답 로그를 기준으로 요청 로그를 찾고 요청 로그가 존재하면 List[Tuple(요청, 응답)] 반환
    for (res <- this.responseLogs; req <- findReqLog(res)) yield (req, res)
  }


  def validate: Boolean = {
    val reqCnt = requestLogs.size
    val resCnt = responseLogs.size
    if (reqCnt == resCnt && resCnt > 0) {
      val pair = pairLogs()
      reqCnt == pair.size
    }
    else false
  }
}


object TraceLog {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val LOG_CHECK_START_MS = 1000 * 5L
  private val LOG_CHECK_INTERVAL_MS = 1000 * 3L
  private val EVICTION_TIME_MS = 1000 * 30L
  private val MAX_CHECK_COUNT = 9

  private def formatter: DateTimeFormatter = {
    dateTimeFormatter
  }
}

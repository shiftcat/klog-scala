package com.example.kstreams.processor

import com.example.kstreams.model.dto.TraceLog
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.{ProcessorContext, Punctuator}
import org.apache.kafka.streams.state.KeyValueStore
import org.slf4j.LoggerFactory

import scala.util.Using
import scala.util.control.Breaks._

class TraceLogPunctuator extends Punctuator {

  private val logger = LoggerFactory.getLogger(getClass)

  private var context: ProcessorContext = _
  private var logsStore: KeyValueStore[String, TraceLog] = _

  def this(context: ProcessorContext, logsStore: KeyValueStore[String, TraceLog]) {
    this()
    this.context = context
    this.logsStore = logsStore
  }


  private def emitLog(key: String, value: TraceLog): Unit = {
    logger.info("emit log [key: {}, size: {}, pair: {}]", key, value.size, value.pairLogs().size)
    logsStore.delete(key)
    context.forward(key, value)
    context.commit()
  }


  override def punctuate(now: Long): Unit = {
    Using(logsStore.all) {iter =>
      while (iter.hasNext) {
        val logsKeyValue: KeyValue[String, TraceLog] = iter.next
        val traceLog: TraceLog = logsKeyValue.value

        breakable {
          if (traceLog == null || traceLog.isEmpty) {
            logsStore.delete(logsKeyValue.key)
            break
          }
          if (traceLog.afterEvictionDatetime(now)) {
            emitLog(logsKeyValue.key, traceLog)
            break
          }
          if (traceLog.beforeCheckDatetime(now)) {
            break
          }

          val isOk: Boolean = traceLog.validate
          traceLog.incrementCheckCount()
          if (isOk) {
            emitLog(logsKeyValue.key, traceLog)
          }
          else {
            traceLog.nextCheckDatetime()
            if (traceLog.maxCheckCount) {
              emitLog(logsKeyValue.key, traceLog)
            }
            else {
              logsStore.put(logsKeyValue.key, traceLog)
            }
          }
        }
      }
    }
  }

}

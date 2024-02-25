package com.example.kstreams.processor

import com.example.kstreams.model.dto.{EventLog, TraceLog}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType}
import org.apache.kafka.streams.state.KeyValueStore

import java.time.Duration

class TraceLogTransformer(stateStoreName: String) extends Transformer[String, EventLog, KeyValue[String, TraceLog]] {

  private var logsStore: KeyValueStore[String, TraceLog] = _


  override def init(context: ProcessorContext): Unit = {
    this.logsStore = context.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[String, TraceLog]]
    val traceLogPunctuator: TraceLogPunctuator = new TraceLogPunctuator(context, logsStore)
    val duration: Duration = Duration.ofMillis(500)
    context.schedule(duration, PunctuationType.WALL_CLOCK_TIME, traceLogPunctuator)
  }


  override def transform(key: String, eventLog: EventLog): KeyValue[String, TraceLog] = {
    var traceLog: TraceLog = logsStore.get(key)
    if (traceLog == null) {
      traceLog = new TraceLog(key, eventLog)
    }
    else {
      traceLog.addLog(eventLog)
    }
    logsStore.put(key, traceLog)
    null
  }


  override def close(): Unit = {
    logsStore.flush()
  }
}


object TraceLogTransformer {
  def supplier(storeName: String): TransformerSupplier[String, EventLog, KeyValue[String, TraceLog]] = {
    () => {
      new TraceLogTransformer(storeName)
    }
  }
}

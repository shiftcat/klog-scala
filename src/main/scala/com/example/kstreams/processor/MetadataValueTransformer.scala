package com.example.kstreams.processor

import com.example.kstreams.model.dto.EventLog
import com.example.kstreams.model.vo.Metadata
import org.apache.kafka.streams.kstream.{ValueTransformer, ValueTransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext

class MetadataValueTransformer extends ValueTransformer[EventLog, EventLog] {

  private var context: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
  }

  override def transform(value: EventLog): EventLog = {
    val metadata = Metadata(
      topic = context.topic(),
      partition = context.partition(),
      offset = context.offset()
    )
    value.metadata = metadata
    value
  }

  override def close(): Unit = {}
}


object MetadataValueTransformer {
  def supplier(): ValueTransformerSupplier[EventLog, EventLog] = {
    new ValueTransformerSupplier[EventLog, EventLog] {
      override def get(): ValueTransformer[EventLog, EventLog] = {
        new MetadataValueTransformer()
      }
    }
  }
}
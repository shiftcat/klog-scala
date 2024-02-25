package com.example.kstreams.processor

import org.apache.kafka.common.header.Header
import org.apache.kafka.streams.kstream.{ValueTransformer, ValueTransformerSupplier}
import org.apache.kafka.streams.processor

import scala.language.implicitConversions

// 출력 토픽 헤더 추가
class OutputTopicHeader[T](headSupplier: (T => Header)*) extends ValueTransformer[T, T] {

  private var context: processor.ProcessorContext = _

  override def init(context: processor.ProcessorContext): Unit = {
    this.context = context
  }

  override def transform(value: T): T = {
    // Output topic에 header 추가
    headSupplier.foreach{f =>
      context.headers().add(f.apply(value))
    }
    value
  }

  override def close(): Unit = {}
}


object OutputTopicHeader {
  implicit def function2ValueTransformerSupplier[T](func: => ValueTransformer[T, T]): ValueTransformerSupplier[T, T] =
    () => func
}

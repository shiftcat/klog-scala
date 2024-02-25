package com.example.kstreams.processor

import com.example.kstreams.model.dto.{LogStatistics, ServiceLog}
import org.apache.kafka.streams.kstream.Aggregator

class LogStatAggregator[T <: AnyRef] extends Aggregator[T, ServiceLog, LogStatistics] {

  override def apply(key: T, value: ServiceLog, aggregate: LogStatistics): LogStatistics = {
    aggregate.key = key
    aggregate.incrementTotalCount()

    val response = value.responseLog.response
    val status = response.status
    if(status >= 200 && status < 300) {
      aggregate.incrementSuccessCount()
    } else {
      if(status >= 300 && status < 500) {
        aggregate.incrementUserErrorCount()
      } else {
        aggregate.incrementServerErrorCount()
      }
    }

    val duration = response.duration
    if(3000 < duration) {
      aggregate.incrementOverThreeCount()
    } else if(1000 < duration) {
      aggregate.incrementOverOneCount()
    }

    aggregate.addDuration(duration)
    aggregate.setMinDuration(duration)
    aggregate.setMaxDuration(duration)
    aggregate.calculateDurationAverage()
    aggregate
  }

}

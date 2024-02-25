package com.example.kstreams.apps


import com.example.kstreams.code.StatType
import com.example.kstreams.model.dto.{LogStatistics, ServiceLog}
import com.example.kstreams.model.vo.{ServiceOperation, StatWindow}
import com.example.kstreams.processor.{LogStatAggregator, OutputTopicHeader}
import com.example.kstreams.serdes.LogStatisticsSerde
import com.sksamuel.avro4s.BinaryFormat
import com.sksamuel.avro4s.kafka.GenericSerde
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.WindowStore
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Properties
import java.util.concurrent.CountDownLatch
import scala.util.Using

object BucketApp extends App {


  private def getProperties: Properties = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "bucket")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[WallclockTimestampExtractor])
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "3")
    props
  }


  private val SOURCE_TOPIC: String = "TOPIC_LOGS"
  private val OUTPUT_TOPIC: String = "TOPIC_STAT"
  private val WINDOW_SIZE = Duration.ofMinutes(1)
  private val WINDOW_GRACE = Duration.ofSeconds(10)

  // 출력 토픽으로 컨텐츠 타입 헤더 추가
  private val contentTypeHeadSupplier = (_: LogStatistics) => {
    new Header {
      override def key(): String = "Content-type"
      override def value(): Array[Byte] = "avro/jaon".getBytes(StandardCharsets.UTF_8)
    }
  }
  // 출력 토픽 Stat-type 헤더 추가
  private val statTypeHeadSupplier = (stat: LogStatistics) => {
    new Header {
      override def key(): String = "Stat-type"
      override def value(): Array[Byte] = stat.statType.toString.getBytes(StandardCharsets.UTF_8)
    }
  }


  // 서비스 통계
  private def serviceStatStream(stream: KStream[String, ServiceLog]): Unit = {
    val keySerde: Serde[String] = Serdes.stringSerde
    val valueSerde: Serde[ServiceLog] = new GenericSerde[ServiceLog](BinaryFormat)
    implicit val grouped: Grouped[String, ServiceLog] = Grouped.`with`(keySerde, valueSerde)

    val statSerde: Serde[LogStatistics] = new LogStatisticsSerde()
    implicit val materialized: Materialized[String, LogStatistics, WindowStore[Bytes, Array[Byte]]] =
      Materialized.`with`(keySerde, statSerde)

    implicit val produced: Produced[Windowed[String], LogStatistics] =
      Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], WINDOW_SIZE.toMillis), new LogStatisticsSerde())

    val aggregator: Aggregator[String, ServiceLog, LogStatistics] =
      new LogStatAggregator[String]

    stream.groupByKey
      .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, WINDOW_GRACE))
      .aggregate(LogStatistics.of(StatType.SERVICE))((k, v, a) => aggregator.apply(k, v, a))
      .suppress(Suppressed.untilWindowCloses(unbounded()))
      .toStream
      .peek((k, v) => v.window = StatWindow.of(k.window.start, k.window.end))
      .peek((k, v) => println(s"Service statistics => $v"))
      .transformValues(new OutputTopicHeader[LogStatistics](contentTypeHeadSupplier, statTypeHeadSupplier))
      .to(OUTPUT_TOPIC)
  }


  // 채널 통계
  private def channelStatStream(stream: KStream[String, ServiceLog]): Unit = {
    val keySerde: Serde[String] = Serdes.stringSerde
    val valueSerde: Serde[ServiceLog] = new GenericSerde[ServiceLog](BinaryFormat)
    implicit val grouped: Grouped[String, ServiceLog] = Grouped.`with`(keySerde, valueSerde)

    val statSerde: Serde[LogStatistics] = new LogStatisticsSerde()
    implicit val materialized: Materialized[String, LogStatistics, WindowStore[Bytes, Array[Byte]]] =
      Materialized.`with`(keySerde, statSerde)

    implicit val produced: Produced[Windowed[String], LogStatistics] =
      Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], WINDOW_SIZE.toMillis), new LogStatisticsSerde())

    val aggregator: Aggregator[String, ServiceLog, LogStatistics] =
      new LogStatAggregator[String]

    stream.groupBy((k, v) => v.responseLog.caller.channel)
      .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, WINDOW_GRACE))
      .aggregate(LogStatistics.of(StatType.CHANNEL))((k, v, a) => aggregator.apply(k, v, a))
      .suppress(Suppressed.untilWindowCloses(unbounded()))
      .toStream
      .peek((k, v) => v.window = StatWindow.of(k.window.start, k.window.end))
      .peek((k, v) => println(s"Channel statistics => $v"))
      .transformValues(new OutputTopicHeader[LogStatistics](contentTypeHeadSupplier, statTypeHeadSupplier))
      .to(OUTPUT_TOPIC)
  }


  // Service-Operation 통계
  private def serviceOperationStatStream(stream: KStream[String, ServiceLog]): Unit = {
    val keySerde: Serde[ServiceOperation] = new GenericSerde[ServiceOperation](BinaryFormat)
    val valueSerde: Serde[ServiceLog] = new GenericSerde[ServiceLog](BinaryFormat)
    implicit val grouped: Grouped[ServiceOperation, ServiceLog] = Grouped.`with`(keySerde, valueSerde)

    val statSerde: Serde[LogStatistics] = new LogStatisticsSerde()
    implicit val materialized: Materialized[ServiceOperation, LogStatistics, WindowStore[Bytes, Array[Byte]]] =
      Materialized.`with`(keySerde, statSerde)

    implicit val produced: Produced[Windowed[ServiceOperation], LogStatistics] =
      Produced.`with`(new WindowedSerdes.TimeWindowedSerde[ServiceOperation](keySerde, WINDOW_SIZE.toMillis), new LogStatisticsSerde())

    val aggregator: Aggregator[ServiceOperation, ServiceLog, LogStatistics] =
      new LogStatAggregator[ServiceOperation]

    stream.groupBy((k, v) => {
        ServiceOperation(k, v.responseLog.operation)
      })
      .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, WINDOW_GRACE))
      .aggregate(LogStatistics.of(StatType.SERVICE_OPERATION))((k, v, a) => aggregator.apply(k, v, a))
      .suppress(Suppressed.untilWindowCloses(unbounded()))
      .toStream
      .peek((k, v) => v.window = StatWindow.of(k.window.start, k.window.end))
      .peek((k, v) => println(s"ServiceOperation statistics => $v"))
      .transformValues(new OutputTopicHeader[LogStatistics](contentTypeHeadSupplier, statTypeHeadSupplier))
      .to(OUTPUT_TOPIC)
  }


  private def createTopology(): Topology = {
    implicit val consumed: Consumed[String, ServiceLog] =
      Consumed.`with`(Serdes.stringSerde, new GenericSerde[ServiceLog](BinaryFormat))
        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST)

    val streamsBuilder: StreamsBuilder = new StreamsBuilder()
    val stream: KStream[String, ServiceLog] =
      streamsBuilder.stream(SOURCE_TOPIC)
        .filter((_, v) => v.validate())

    serviceStatStream(stream)
    channelStatStream(stream)
    serviceOperationStatStream(stream)

    streamsBuilder.build()
  }


  Using(new KafkaStreams(createTopology(), getProperties)) { stream =>
    val latch = new CountDownLatch(1)
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      stream.close()
      System.out.println("System... close.")
      latch.countDown()
    }))
    stream.start()
    latch.await()
  }

}

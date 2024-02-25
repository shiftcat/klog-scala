package com.example.kstreams.apps

import com.example.kstreams.model.dto.{EventLog, ServiceLog, TraceLog}
import com.example.kstreams.processor.{MetadataValueTransformer, OutputTopicHeader, TraceLogTransformer}
import com.example.kstreams.serdes.EventLogJsonSerde
import com.sksamuel.avro4s.BinaryFormat
import com.sksamuel.avro4s.kafka.GenericSerde
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Branched, Consumed, KStream, Produced}
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, Stores}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.CountDownLatch
import scala.language.implicitConversions
import scala.util.Using


object FunnelApp extends App {


  private val SOURCE_TOPIC = "EVENT_LOG"
  private val OUTPUT_TOPIC = "TOPIC_LOGS"


  private def getProperties: Properties = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "funnel")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[WallclockTimestampExtractor])
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "3")
    props
  }


  implicit val consumed: Consumed[String, EventLog] = Consumed.`with`(Serdes.String(), new EventLogJsonSerde())
  implicit val produced: Produced[String, ServiceLog] = Produced.`with`(Serdes.String(), new GenericSerde[ServiceLog](BinaryFormat))


  // 성공처리 스트림
  private def successStream(stream: KStream[String, TraceLog]): Unit = {
    // 출력 토픽으로 컨텐츠 타입 헤더 추가
    val contentTypeHeadSupplier = (_: ServiceLog) => {
      new Header {
        override def key(): String = "Content-type"
        override def value(): Array[Byte] = "avro/binary".getBytes(StandardCharsets.UTF_8)
      }
    }

    stream
      .flatMapValues({ t =>
        val pair = t.pairLogs()
        pair.map(p => ServiceLog(t.traceId, p._1.service, p._1, p._2))
      })
      .selectKey((_, v) => v.service)
      .peek { (k, v) => println(s"Success to topic => key: $k, value: $v") }
      .transformValues(new OutputTopicHeader[ServiceLog](contentTypeHeadSupplier))
      .to(OUTPUT_TOPIC)
  }


  // 실패처리 스트림
  private def failureStream(stream: KStream[String, TraceLog]): Unit = {
    stream.print(Printed.toSysOut)
  }


  // 토폴로지 생성
  private def createTopology(): Topology = {
    val traceLogSerde = new GenericSerde[TraceLog](BinaryFormat)

    val keyValueStore: KeyValueBytesStoreSupplier = Stores.inMemoryKeyValueStore("logs-store")
    val storeBuilder: StoreBuilder[KeyValueStore[String, TraceLog]] =
      Stores.keyValueStoreBuilder(keyValueStore, Serdes.String, traceLogSerde)

    val streamsBuilder: StreamsBuilder = new StreamsBuilder()
    streamsBuilder.addStateStore(storeBuilder)

    streamsBuilder.stream(SOURCE_TOPIC)
      .filter((_, v) => v.validate)
      .transformValues(MetadataValueTransformer.supplier())
      // .peek { (k, v) => println(s"key: $k, value: $v, metadata: ${v.metadata}")}
      .transform(TraceLogTransformer.supplier(keyValueStore.name()), keyValueStore.name())
      .filter((_, v) => !v.isEmpty)
      //      .peek { (k, v) => println(s"key: $k, value: $v") }
      .split()
      .branch((_, v) => v.validate, Branched.withConsumer(s => successStream(s)))
      .branch((_, v) => !v.validate, Branched.withConsumer(s => failureStream(s)))
      .noDefaultBranch()

    streamsBuilder.build()
  }


  Using(new KafkaStreams(createTopology(), getProperties)) {
    stream => {
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

}

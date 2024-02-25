package com.example.kstreams.model.dto

import com.example.kstreams.code.StatType
import com.example.kstreams.code.StatType._
import com.example.kstreams.model.vo.{ServiceOperation, StatWindow}
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.{GenericContainer, GenericData, GenericRecord}
import org.apache.avro.{Schema, SchemaBuilder}

case class LogStatistics(
  statType: StatType, var key: AnyRef, var window: StatWindow,
  private var totalCount: Long, private var successCount: Long, private var userErrorCount: Long, private var serverErrorCount: Long,
  private var overOneCount: Long, private var overThreeCount: Long,
  private var minDuration: Long, private var maxDuration: Long, private var totalDuration: Long, private var durationAverage: Double
) extends GenericContainer {

  def this(statType: StatType) = {
    this(statType, null, null, 0, 0, 0, 0, 0, 0, Long.MaxValue, 0, 0, 0.0)
  }

  def incrementTotalCount(): Unit = {
    this.totalCount += 1
  }

  def incrementSuccessCount(): Unit = {
    this.successCount += 1
  }

  def incrementUserErrorCount(): Unit = {
    this.userErrorCount += 1
  }

  def incrementServerErrorCount(): Unit = {
    this.serverErrorCount += 1
  }

  def incrementOverOneCount(): Unit = {
    this.overOneCount += 1
  }

  def incrementOverThreeCount(): Unit = {
    this.overThreeCount += 1
  }

  def setMinDuration(duration: Long): Unit = {
    if (this.minDuration > duration) this.minDuration = duration
  }

  def setMaxDuration(duration: Long): Unit = {
    if (this.maxDuration < duration) this.maxDuration = duration
  }

  def addDuration(duration: Long): Unit = {
    this.totalDuration += duration
  }

  def calculateDurationAverage(): Unit = {
    this.durationAverage = this.totalDuration / this.totalCount.toDouble
  }



  private def keyRecord(statType: StatType): AnyRef = {
    statType match {
      case StatType.SERVICE => this.key.asInstanceOf[String]
      case StatType.CHANNEL => this.key.asInstanceOf[String]
      case StatType.SERVICE_OPERATION =>
        val so = key.asInstanceOf[ServiceOperation]
        so.toGenericRecord
    }
  }


  def toGenericRecord: GenericRecord = {
    val schema = LogStatistics.schema(statType)
    val record = new GenericData.Record(schema)
    record.put("statType", new GenericData.EnumSymbol(StatType.schema(), statType.toString))
    record.put("key", keyRecord(statType))
    if(this.window != null) {
      record.put("window", this.window.toGenericRecord())
    } else {
      record.put("window", null)
    }

    record.put("totalCount", this.totalCount)
    record.put("successCount", this.successCount)
    record.put("userErrorCount", this.userErrorCount)
    record.put("serverErrorCount", this.serverErrorCount)

    record.put("overOneCount", this.overOneCount)
    record.put("overThreeCount", this.overThreeCount)

    record.put("minDuration", this.minDuration)
    record.put("maxDuration", this.maxDuration)
    record.put("totalDuration", this.totalDuration)
    record.put("durationAverage", this.durationAverage)
    record
  }


  override def getSchema: Schema = LogStatistics.schema(this.statType)
}


object LogStatistics {

  def of(statType: StatType): LogStatistics = {
    new LogStatistics(statType)
  }

  private def schemaOfStatKey(statType: StatType): Schema = {
    statType match {
      case StatType.SERVICE => SchemaBuilder.builder().stringType()
      case StatType.CHANNEL => SchemaBuilder.builder().stringType()
      case StatType.SERVICE_OPERATION => ServiceOperation.schema()
      case _ => SchemaBuilder.builder().bytesType()
    }
  }

  def schema(statType: StatType): Schema = {
    val schemaBuilder =
      SchemaBuilder.record(LogStatistics.getClass.getSimpleName.replace("""$""", ""))
        .namespace(LogStatistics.getClass.getPackageName)
        .fields()

    schemaBuilder
      .name("statType").`type`(StatType.schema()).noDefault()
      .name("key").`type`(schemaOfStatKey(statType)).noDefault()
      .name("window").`type`(SchemaBuilder.nullable().`type`(StatWindow.schema())).noDefault()
      .requiredLong("totalCount")
      .requiredLong("successCount")
      .requiredLong("userErrorCount")
      .requiredLong("serverErrorCount")
      .requiredLong("overOneCount")
      .requiredLong("overThreeCount")
      .requiredLong("minDuration")
      .requiredLong("maxDuration")
      .requiredLong("totalDuration")
      .requiredDouble("durationAverage")
    schemaBuilder.endRecord()
  }


  def apply(record: GenericRecord): LogStatistics = {
    val statTypeSymbol = record.get("statType").ensuring(_.isInstanceOf[EnumSymbol]).asInstanceOf[EnumSymbol]
    val statType = StatType(statTypeSymbol.toString).orNull
    val key = statType match {
      case SERVICE => record.get("key").toString
      case CHANNEL => record.get("key").toString
      case SERVICE_OPERATION =>
        val keyRecord = record.get("key").ensuring(_.isInstanceOf[GenericRecord]).asInstanceOf[GenericRecord]
        val service = keyRecord.get("service").toString
        val operation = keyRecord.get("operation").toString
        ServiceOperation(service, operation)
    }

    val windowRef = record.get("window")
    val window = if(windowRef != null) {
      val windowRecord = windowRef.ensuring(_.isInstanceOf[GenericRecord]).asInstanceOf[GenericRecord]
      val start = windowRecord.get("start").ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]
      val end = windowRecord.get("end").ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]
      StatWindow(start, end)
    } else {
      null
    }

    val totalCount = record.get("totalCount").ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]
    val successCount = record.get("successCount").ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]
    val userErrorCount = record.get("userErrorCount").ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]
    val serverErrorCount = record.get("serverErrorCount").ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]

    val overOneCount = record.get("overOneCount").ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]
    val overThreeCount = record.get("overThreeCount").ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]

    val minDuration = record.get("minDuration").ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]
    val maxDuration = record.get("maxDuration").ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]
    val totalDuration = record.get("totalDuration").ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]
    val durationAverage = record.get("durationAverage").ensuring(_.isInstanceOf[Double]).asInstanceOf[Double]

    LogStatistics(statType, key, window, totalCount, successCount, userErrorCount, serverErrorCount, overOneCount, overThreeCount, minDuration, maxDuration, totalDuration, durationAverage)
  }


}

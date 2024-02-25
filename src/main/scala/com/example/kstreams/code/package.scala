package com.example.kstreams

import org.apache.avro.{Schema, SchemaBuilder}


package object code {

  object LogType extends Enumeration with Serializable {
    type LogType = Value
    val REQ: code.LogType.Value = Value(1, "REQ")
    val RES: code.LogType.Value = Value(2, "RES")

    implicit val enum: LogType.type = LogType

    def apply(code: String): Option[Value] = {
      values.find(_.toString == code)
    }

    def schema(): Schema = {
      SchemaBuilder.enumeration("statTypeEnum")
        .symbols(LogType.values.map(_.toString).toList:_*)
    }
  }


  object StatType extends Enumeration {
    type StatType = Value
    val SERVICE: code.StatType.Value = Value(1, "SERVICE")
    val CHANNEL: code.StatType.Value = Value(2, "CHANNEL")
    val SERVICE_OPERATION: code.StatType.Value = Value(3, "SERVICE_OPERATION")

    implicit val enum: StatType.type = StatType

    def apply(code: String): Option[Value] = {
      values.find(_.toString == code)
    }

    def schema(): Schema = {
      SchemaBuilder.enumeration("statTypeEnum")
        .symbols(StatType.values.map(_.toString).toList:_*)
    }
  }


}

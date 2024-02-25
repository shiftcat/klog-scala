package com.example.kstreams.utils

import com.google.common.base.Throwables
import org.json.{JSONArray, JSONObject}
import org.slf4j.LoggerFactory

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

class JsonUtil(json: JSONObject) {

  private val logger = LoggerFactory.getLogger(getClass)

  private def hasKeyAndNotNullValue(key: String): Boolean = {
    json.has(key) && !json.isNull(key)
  }


  private def tryWithExtract[T](key: String)(extractor: String => T): Option[T] = {
    if(hasKeyAndNotNullValue(key)) {
      return Try(extractor(key)) match {
        case Success(value) => Some(value)
        case Failure(ex) =>
          logger.error(Throwables.getStackTraceAsString(ex))
          None
      }
    }
    None
  }



  def getJSONObject(key: String): Option[JSONObject] = {
    tryWithExtract(key){
      // JSONObject.getJSONObject(key)
      json.getJSONObject
    }
  }


  def getJsonArray(key: String): Option[JSONArray] = {
    tryWithExtract(key){
      // JSONObject.getJSONArray(key)
      json.getJSONArray
    }
  }


  def getString(key: String): Option[String] = {
    tryWithExtract(key){
      // JSONObject.getString(key)
      json.getString
    }
  }


  def getInt(key: String): Option[Int] = {
    tryWithExtract(key){
      json.getInt
    }
  }


  def getLong(key: String): Option[Long] = {
    tryWithExtract(key){
      json.getLong
    }
  }


  def getDouble(key: String): Option[Double] = {
    tryWithExtract(key){
      json.getDouble
    }
  }

}



object JsonUtil {

  def apply(json: JSONObject): JsonUtil = {
    new JsonUtil(json)
  }

  implicit def jsonObject2JsonUtil(json: JSONObject): JsonUtil = new JsonUtil(json)


  implicit final class ValueExtractor(private val util: JsonUtil) {
    def enumValue[T <: Enumeration](key: String)(implicit enum: T): Option[T#Value] = {
      util.getString(key).map(enum.withName)
    }

    def extractValue[T](key: String)(implicit extractor: Option[JSONObject] => T): T = {
      extractor.apply(util.getJSONObject(key))
    }

    def extractString(key: String)(implicit extractor: Option[String] => String): String = {
      extractor.apply(util.getString(key))
    }

    def extractLong(key: String)(implicit extractor: Option[Long] => Long): Long = {
      extractor.apply(util.getLong(key))
    }
  }

}
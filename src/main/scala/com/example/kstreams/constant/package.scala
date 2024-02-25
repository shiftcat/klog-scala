package com.example.kstreams

import java.time.ZoneId
import java.time.format.DateTimeFormatter

package object constant {
  def dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").withZone(ZoneId.systemDefault)
}

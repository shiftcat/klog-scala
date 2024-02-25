package com.example.kstreams.model.dto

import org.apache.commons.lang3.StringUtils

case class ServiceLog(traceId: String, service: String,
                      requestLog: RequestEventLog, responseLog: ResponseEventLog) {

  def validate(): Boolean = {
    if (StringUtils.isEmpty(this.traceId)) return false
    if (StringUtils.isEmpty(this.service)) return false
    if (this.requestLog == null || !this.requestLog.validate) return false
    if (this.responseLog == null || !this.responseLog.validate) return false
    true
  }

}


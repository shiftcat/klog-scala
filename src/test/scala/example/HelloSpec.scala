package example

import com.example.kstreams.code.StatType
import com.example.kstreams.model.dto.LogStatistics
import com.example.kstreams.model.vo.ServiceOperation
import org.scalatest.funsuite.AnyFunSuite

class HelloSpec extends AnyFunSuite {

  test("stat window avro") {
//    val logstat = LogStatistics(StatType.SERVICE_OPERATION, ServiceOperation("service", "operation"),
//      StatWindow(1000, 1000), 1000, 100, 10, 5, 3, 1, 100000, 10000, 10000000, 30.22
//    )
    val logstat = LogStatistics(StatType.SERVICE_OPERATION, ServiceOperation("service", "operation"),
      null, 1000, 100, 10, 5, 3, 1, 100000, 10000, 10000000, 30.22
    )

    logstat.toGenericRecord
  }
}

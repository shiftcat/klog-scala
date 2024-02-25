import sbt.*

object Dependencies {
  val avroVersion = "1.11.3"
  val avro4sVersion = "4.1.1"
//  val kafkaVersion = "3.4.0"
   val kafkaVersion = "3.1.0"
  val jacksonVersion = "2.16.1"

  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.17" % Test
  val java8Compat = "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2"
  val kafkaStreams = "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion

  // https://index.scala-lang.org/pjfanning/jackson-module-scala-duration
  val jacksonModule = "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
  val jacksonDatatype = "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion

  val avroCompiler = "org.apache.avro" % "avro-compiler" % avroVersion
  val avro = "org.apache.avro" % "avro" % avroVersion
  // https://index.scala-lang.org/sksamuel/avro4s
  val avro4sCore = "com.sksamuel.avro4s" %% "avro4s-core" % avro4sVersion
  val avro4sKafka = "com.sksamuel.avro4s" %% "avro4s-kafka" % avro4sVersion

  // 위와 다르게 '%'를 사용한 이유는 '%%'를 사용하면 xxx-<scala-version>을 찾으려고 한다.
  // 모듈이 스칼라 버전을 제공하지 않는다면 '%'를 사용하여 자바용 버전으로 사용
  val json = "org.json" % "json" % "20231013"
  val commonsLang = "org.apache.commons" % "commons-lang3" % "3.14.0"
  val guava = "com.google.guava" % "guava" % "31.1-jre"
}

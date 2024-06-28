import Dependencies._

ThisBuild / scalaVersion := "2.13.12"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"
ThisBuild / organizationName := "klog"

lazy val root = (project in file("."))
  .settings(
    name := "klog-scala",

    libraryDependencies += scalaTest,
    libraryDependencies += kafkaStreams,
    libraryDependencies += jacksonModule,
    libraryDependencies += jacksonDatatype,
    libraryDependencies += json,
    libraryDependencies += commonsLang,
    libraryDependencies += guava,
    libraryDependencies += java8Compat,
    libraryDependencies += avro,
    libraryDependencies += avroCompiler,
    libraryDependencies += avro4sCore,
    libraryDependencies += avro4sKafka,
    libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "2.0.5",
      "ch.qos.logback" % "logback-classic" % "1.4.7")
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.

// deduplicate: different file contents found in the following:
// versions/9/module-info.class`
assembly / assemblyMergeStrategy := {
  case PathList("module-info.class") => MergeStrategy.concat
  case path if path.endsWith("module-info.class") => MergeStrategy.concat
//  case path if path.endsWith("META-INF/versions/9/module-info.class") => MergeStrategy.concat
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

assemblyOutputPath in assembly := file("target/klog-scala.jar")
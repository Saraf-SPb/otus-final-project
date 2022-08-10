name := "otus-final-project"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.2.1"
val vegasVersion = "0.3.11"
val postgresVersion = "42.2.2"
val scalaTestVersion = "3.2.1"
val flinkVersion = "1.12.1"
val cassandraConnectorVersion = "3.2.0"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",

  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,

  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core"   % "2.12.0",
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.12.0",

  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,
  "org.postgresql" % "postgresql" % postgresVersion,

  "joda-time" % "joda-time" % "2.10.13",

  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  // https://mvnrepository.com/artifact/org.apache.flink/flink-java
  "org.apache.flink" % "flink-java" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-java" % flinkVersion,
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-runtime-web" % flinkVersion,
  "org.apache.flink" %% "flink-cep" % flinkVersion,
  "org.apache.flink" %% "flink-cep-scala" % flinkVersion,
  "org.apache.flink" %% "flink-state-processor-api" % flinkVersion,
  "org.apache.flink" %% "flink-table-uber" % flinkVersion,
  "org.apache.flink" % "flink-test-utils-junit" % flinkVersion,
  "org.apache.flink" %% "flink-test-utils" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-java" % flinkVersion,
  "org.apache.flink" %% "flink-runtime" % flinkVersion,
  "com.typesafe"      % "config"      % "1.4.2",
  "io.circe" %% "circe-core"  % "0.11.1",
  "io.circe" %% "circe-generic"  % "0.11.1",
  "io.circe" %% "circe-parser" % "0.11.1",
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.12.0"
)
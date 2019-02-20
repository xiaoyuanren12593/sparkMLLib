import sbt._

object Dependencies {

  //  hive
  val sparkHive = "org.apache.spark" %% "spark-hive" % "1.6.1"
  //  spark-neo4j
  val sparkNeo4j = "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4"
  //  spark-csv
  val sparkCsv = "com.databricks" %% "spark-csv" % "1.4.0"
  // sparkStreaming
  val sparkStreaming = "org.apache.spark" %% "spark-streaming" % "1.6.1"
  val sparkStreamingKafka = "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1"
  // spark-mllib
  val sparkMllib = "org.apache.spark" % "spark-mllib_2.10" % "1.6.1"
  //  hbase
  val hbaseClient = "org.apache.hbase" % "hbase-client" % "1.2.0"
  val hbaseCommon = "org.apache.hbase" % "hbase-common" % "1.2.0"
  val hbaseServer = "org.apache.hbase" % "hbase-server" % "1.2.0"
  val hbaseHadoopCompat = "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.0"

  // 其他Jar
  val neo4jJavaDriver = "org.neo4j.driver" % "neo4j-java-driver" % "1.0.4"
  val ansjSeg = "org.ansj" % "ansj_seg" % "5.1.1"
  val nlpLang = "org.nlpcn" % "nlp-lang" % "1.7.2"
  val mysqlConnectorJava = "mysql" % "mysql-connector-java" % "5.1.36"
  val jedis = "redis.clients" % "jedis" % "2.9.0"

  // 编译时用
  val commonDeps = Seq(sparkHive, sparkNeo4j, sparkCsv, sparkStreaming, sparkStreamingKafka, sparkMllib,
    hbaseClient, hbaseCommon, hbaseServer, hbaseHadoopCompat, neo4jJavaDriver, ansjSeg, nlpLang, mysqlConnectorJava, jedis)

  //---------------------------------------------以下打包使用------------------------------------------------------------
  //  hive
  val sparkHiveProvided = "org.apache.spark" %% "spark-hive" % "1.6.1" % "provided"
  //  spark-neo4j
  val sparkNeo4jProvided = "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4" % "provided"
  //  spark-csv
  val sparkCsvProvided = "com.databricks" %% "spark-csv" % "1.4.0" % "provided"
  // sparkStreaming
  val sparkStreamingProvided = "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided"
  val sparkStreamingKafkaProvided = "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1" % "provided"
  // spark-mllib
  val sparkMllibProvided = "org.apache.spark" % "spark-mllib_2.10" % "1.6.1" % "provided"
  //  hbase
  val hbaseClientProvided = "org.apache.hbase" % "hbase-client" % "1.2.0" % "provided"
  val hbaseCommonProvided = "org.apache.hbase" % "hbase-common" % "1.2.0" % "provided"
  val hbaseServerProvided = "org.apache.hbase" % "hbase-server" % "1.2.0" % "provided"
  val hbaseHadoopCompatProvided = "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.0" % "provided"

  // 其他Jar
  val neo4jJavaDriverProvided = "org.neo4j.driver" % "neo4j-java-driver" % "1.0.4" % "provided"
  val ansjSegProvided = "org.ansj" % "ansj_seg" % "5.1.1" % "provided"
  val nlpLangProvided = "org.nlpcn" % "nlp-lang" % "1.7.2" % "provided"
  val mysqlConnectorJavaProvided = "mysql" % "mysql-connector-java" % "5.1.36" % "provided"
  val jedisProvided = "redis.clients" % "jedis" % "2.9.0" % "provided"

  // 打包时用
  val commonProvidedDeps = Seq(sparkHiveProvided, sparkNeo4jProvided, sparkCsvProvided,
    sparkStreamingProvided, sparkStreamingKafkaProvided, sparkMllibProvided, hbaseClientProvided, hbaseCommonProvided,
    hbaseServerProvided, hbaseHadoopCompatProvided, neo4jJavaDriverProvided, ansjSegProvided, nlpLangProvided,
    mysqlConnectorJavaProvided, jedisProvided)
}

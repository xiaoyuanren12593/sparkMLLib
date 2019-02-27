import sbt._

object Dependencies {

  // 数据库连接 Jar
  val neo4jJavaDriver = "org.neo4j.driver" % "neo4j-java-driver" % "1.0.4"
  val mysqlConnectorJava = "mysql" % "mysql-connector-java" % "5.1.36"
  val jedis = "redis.clients" % "jedis" % "2.9.0"
  //  Alibaba-json
  val fastjson = "com.alibaba" % "fastjson" % "1.2.24"
  // joda-time
  val jodaTime = "joda-time" % "joda-time" % "2.3"
  // spark 中文分词
  val ansjSeg = "org.ansj" % "ansj_seg" % "5.1.1"
  val nlpLang = "org.nlpcn" % "nlp-lang" % "1.7.2"
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

  //--------------------------------------------全依赖-----------------------------------------------------------------
  val allDepsProvided = Seq(neo4jJavaDriver, mysqlConnectorJava, jedis, fastjson, ansjSeg, nlpLang,
    sparkHiveProvided, sparkNeo4jProvided, sparkCsvProvided, sparkStreamingProvided,
    sparkStreamingKafkaProvided, sparkMllibProvided, hbaseClientProvided, hbaseCommonProvided, hbaseServerProvided,
    hbaseHadoopCompatProvided)

  //--------------------------------------------工具模块----------------------------------------------------------------
  val utilDeps = Seq(jodaTime)

  //---------------------------------------------企业模块标签------------------------------------------------------------
  val enterpriseDepsProvided = Seq(mysqlConnectorJava, fastjson, jodaTime, sparkHiveProvided, hbaseClientProvided,
    hbaseServerProvided, hbaseCommonProvided)

  //---------------------------------------------企业价值个人风险标签------------------------------------------------------------
  val entvaluePersonrisklDepsProvided = Seq(fastjson, sparkHiveProvided, sparkMllibProvided, hbaseClientProvided,
    hbaseServerProvided, hbaseCommonProvided)

  //---------------------------------------------etl bi3模块标签-------------------------------------------------------
  val etlBi3DepsProvided = Seq(mysqlConnectorJava, fastjson, sparkHiveProvided, sparkMllibProvided,
    hbaseClientProvided, hbaseServerProvided, hbaseCommonProvided)

  //---------------------------------------------etl piwik模块标签-------------------------------------------------------
  val etlPiwikDepsProvided = Seq(mysqlConnectorJava, sparkHiveProvided, sparkMllibProvided, hbaseClientProvided,
    hbaseServerProvided, hbaseCommonProvided)

  //---------------------------------------------etl redis模块标签-------------------------------------------------------
  val etlRedisDepsProvided = Seq(mysqlConnectorJava, neo4jJavaDriver, fastjson, jedis, ansjSeg,
    sparkStreamingKafkaProvided, sparkHiveProvided, sparkMllibProvided, hbaseClientProvided,
    hbaseServerProvided, hbaseCommonProvided)

  //---------------------------------------------个人模块标签------------------------------------------------------------
  val personalDepsProvided = Seq(fastjson, sparkHiveProvided, hbaseClientProvided, hbaseServerProvided, hbaseCommonProvided)
}

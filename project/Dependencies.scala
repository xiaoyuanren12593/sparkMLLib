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
  //---------------------------------------------以上打包使用------------------------------------------------------------

  //--------------------------------------------全依赖-----------------------------------------------------------------
  val allDeps = Seq(neo4jJavaDriver, mysqlConnectorJava, jedis, fastjson, ansjSeg, nlpLang,
    sparkHive, sparkNeo4j, sparkCsv, sparkStreaming, sparkStreamingKafka, sparkMllib, hbaseClient,
    hbaseCommon, hbaseServer, hbaseHadoopCompat).map(
    _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
  ).map(
    _.excludeAll(ExclusionRule(organization = "javax.servlet"))
  )

  val allDepsProvided = Seq(neo4jJavaDriver, mysqlConnectorJava, jedis, fastjson, ansjSeg, nlpLang,
    sparkHiveProvided, sparkNeo4jProvided, sparkCsvProvided, sparkStreamingProvided,
    sparkStreamingKafkaProvided, sparkMllibProvided, hbaseClientProvided, hbaseCommonProvided, hbaseServerProvided,
    hbaseHadoopCompatProvided)

  //--------------------------------------------工具模块----------------------------------------------------------------
  val utilDeps = Seq(jodaTime)

  //---------------------------------------------企业模块标签------------------------------------------------------------
  // 企业标签打包时需要的引用
  val enterpriseDeps = Seq(mysqlConnectorJava, fastjson, jodaTime, sparkHive, hbaseClient, hbaseServer, hbaseCommon)
  // 企业标签本地引用
  val enterpriseDepsProvided = Seq(mysqlConnectorJava, fastjson, jodaTime, sparkHiveProvided, hbaseClientProvided,
    hbaseServerProvided, hbaseCommonProvided)

  //---------------------------------------------个人模块标签------------------------------------------------------------
  // 个人标签本地引用
  val entvaluePersonrisklDeps = Seq(fastjson, sparkHive, sparkMllib, hbaseClient, hbaseServer, hbaseCommon)
  // 个人标签打包时需要的引用
  val entvaluePersonrisklDepsProvided = Seq(fastjson, sparkHive, sparkMllibProvided, hbaseClientProvided,
    hbaseServerProvided, hbaseCommonProvided)

  //---------------------------------------------etl bi3模块标签-------------------------------------------------------
  // 个人标签打包时需要的引用
  val etlBi3Deps = Seq(mysqlConnectorJava, fastjson, sparkHive, sparkMllib, hbaseClient, hbaseServer, hbaseCommon)
  // 个人标签本地引用
  val etlBi3DepsProvided = Seq(mysqlConnectorJava, fastjson, sparkHiveProvided, sparkMllibProvided,
    hbaseClientProvided, hbaseServerProvided, hbaseCommonProvided)

  //---------------------------------------------etl piwik模块标签-------------------------------------------------------
  // 个人标签打包时需要的引用
  val etlPiwikDeps = Seq(mysqlConnectorJava, sparkHive, sparkMllib, hbaseClient, hbaseServer, hbaseCommon)
  // 个人标签本地引用
  val etlPiwikDepsProvided = Seq(mysqlConnectorJava, sparkHiveProvided, sparkMllibProvided, hbaseClientProvided,
    hbaseServerProvided, hbaseCommonProvided)

  //---------------------------------------------etl redis模块标签-------------------------------------------------------
  // 个人标签打包时需要的引用
  val etlRedisDeps = Seq(neo4jJavaDriver, mysqlConnectorJava, jedis, fastjson, ansjSeg, sparkStreaming, 
    sparkStreamingKafka, sparkMllib, sparkHive, sparkMllib)
  // 个人标签本地引用
  val etlRedisDepsProvided = Seq(mysqlConnectorJava, sparkHiveProvided, sparkMllibProvided, hbaseClientProvided,
    hbaseServerProvided, hbaseCommonProvided)

  //---------------------------------------------个人模块标签------------------------------------------------------------
  // 企业标签打包时需要的引用
  val personalDeps = Seq(fastjson, sparkHive, hbaseClient, hbaseServer, hbaseCommon)
  // 企业标签本地引用
  val personalDepsProvided = Seq(fastjson, sparkHiveProvided, hbaseClientProvided, hbaseServerProvided, hbaseCommonProvided)
}

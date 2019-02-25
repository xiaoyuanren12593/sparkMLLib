import Dependencies._
import sbtassembly.AssemblyPlugin.autoImport.assemblyOption

// 添加非托管依赖的jar
unmanagedBase := baseDirectory.value / "lib"
unmanagedJars in Compile := Seq.empty[sbt.Attributed[java.io.File]]

//resolvers +=
resolvers ++= Seq(
  "Restlet Repositories" at "http://maven.restlet.org",
  "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven",
  "bintray-sbt-plugins" at "http://dl.bintray.com/sbt/sbt-plugin-releases",
  "central" at "https://maven.aliyun.com/repository/central"
)


//
////libraryDependencies ++= Seq(
////
//).map(
//  _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
//).map(
//  _.excludeAll(ExclusionRule(organization = "javax.servlet"))
//)

// 公共配置
val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.10.4",
  //挡在java项目中写中文时，编译会报错，加上该行就行了
  javacOptions ++= Seq("-encoding", "UTF-8")
//挡在java项目中写中文时，编译会报错，加上该行就行了
javacOptions ++= Seq("-encoding", "UTF-8")

libraryDependencies ++= Seq(
  //  通过驱动器来实现neo4j(写sql)
  //  "org.neo4j.driver" % "neo4j-java-driver" % "1.0.4",
  "org.neo4j.driver" % "neo4j-java-driver" % "1.0.4" % "provided",

  //  Alibaba-json
  "com.alibaba" % "fastjson" % "1.2.24",

  /**
    * saprk对中文进行分词
    * https://mvnrepository.com/artifact/org.ansj/ansj_seg
    * https://mvnrepository.com/artifact/org.nlpcn/nlp-lang
    **/
  "org.ansj" % "ansj_seg" % "5.1.1",
  "org.nlpcn" % "nlp-lang" % "1.7.2",
  //  "org.ansj" % "ansj_seg" % "5.1.1" % "provided",
  //  "org.nlpcn" % "nlp-lang" % "1.7.2" % "provided",

  //  mysql connect
  "mysql" % "mysql-connector-java" % "5.1.36",
  //  "mysql" % "mysql-connector-java" % "5.1.36" % "provided",

  "joda-time" % "joda-time" % "2.9.9",

  //  redis-client
  "redis.clients" % "jedis" % "2.9.0",
  //  "redis.clients" % "jedis" % "2.9.0" % "provided",

  // ===================================================================
//        spark-neo4j
//      "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4",
//  // spark-csv
//  "com.databricks" %% "spark-csv" % "1.4.0",
//  //  hive
//  "org.apache.spark" %% "spark-hive" % "1.6.1",
//  // spark-mllib
//  "org.apache.spark" % "spark-mllib_2.10" % "1.6.1",
//  // sparkStreaming
//  "org.apache.spark" %% "spark-streaming" % "1.6.1",
//  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1",
//  //  hbase
//  "org.apache.hbase" % "hbase-client" % "1.2.0",
//  "org.apache.hbase" % "hbase-common" % "1.2.0",
//  "org.apache.hbase" % "hbase-server" % "1.2.0",
//  "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.0"
  // =========================================================================================

//    hive
  "org.apache.spark" %% "spark-hive" % "1.6.1" % "provided",
  //  spark-neo4j
  "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4" % "provided",
  //  spark-csv
  "com.databricks" %% "spark-csv" % "1.4.0" % "provided",
  // sparkStreaming
  "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1" % "provided",
  // spark-mllib
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.1" % "provided",
  //  hbase
  "org.apache.hbase" % "hbase-client" % "1.2.0" % "provided",
  "org.apache.hbase" % "hbase-common" % "1.2.0" % "provided",
  "org.apache.hbase" % "hbase-server" % "1.2.0" % "provided",
  "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.0" % "provided"

).map(
  _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
).map(
  _.excludeAll(ExclusionRule(organization = "javax.servlet"))
>>>>>>> master
)

// 公共的 打包 配置
val commonAssemblySettings = Seq(
  //解决依赖重复的问题
  assemblyMergeStrategy in assembly := {
    case PathList(ps@_*) if ps.last endsWith ".properties" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith "Absent.class" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".xml" => MergeStrategy.first
    case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  //执行assembly的时候忽略测试
  test in assembly := {},
  //把scala本身排除在Jar中，因为spark已经包含了scala
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
)

// 主工程
lazy val bznSparkNeed = (project in file("."))
  .aggregate(enterprise, personal)
  .settings(
    libraryDependencies ++= enterpriseDeps)
  .settings(
    name := "bzn_spark_need"
  )

// 事例项目
lazy val enterprise = (project in file("enterprise"))
  .settings(
    libraryDependencies ++= enterpriseDeps)
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //指定类的名字
    //    mainClass in assembly := Some("com.ladder.example.hive.SparkHiveExample"),
    //定义jar包的名字
    assemblyJarName in assembly := "sbt-enterprise.jar"
  )

// 事例项目
lazy val personal = (project in file("personal"))
  .settings(
    libraryDependencies ++= personalDeps)
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //指定类的名字
    //    mainClass in assembly := Some("com.ladder.example.hive.SparkHiveExample"),
    //定义jar包的名字
    assemblyJarName in assembly := "sbt-personal.jar"
  )

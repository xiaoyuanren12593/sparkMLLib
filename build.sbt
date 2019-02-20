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
lazy val sparkLadder = (project in file("."))
  .aggregate(enterprise, personal)
  .settings(
    libraryDependencies ++= commonDeps)
  .settings(
    name := "bzn_spark_need"
  )

// 事例项目
lazy val enterprise = (project in file("enterprise"))
  .settings(
    libraryDependencies ++= enterpriseProvidedDeps)
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

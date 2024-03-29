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

// 公共配置
val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.10.4",
  //挡在java项目中写中文时，编译会报错，加上该行就行了
  javacOptions ++= Seq("-encoding", "UTF-8"),
  // the 'run' task uses all the libraries, including the ones marked with "provided".
  run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated,
  runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated
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
  .settings(
    libraryDependencies ++= allDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    name := "bznSparkNeed"
  )

// 事例项目
lazy val jobUtil = (project in file("job-util"))
  .settings(
    libraryDependencies ++= utilDeps)
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bzn-util.jar"
  )

// 企业相关
lazy val jobEnterprise = (project in file("job-enterprise"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= enterpriseDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bzn-label-enterprise.jar"
  )


// 企业价值与个人风险
lazy val jobEntValuePersonRisk = (project in file("job-entvalue-personrisk"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= entvaluePersonrisklDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bzn-entvalue-personrisk.jar"
  )

// 企业价值与个人风险
lazy val jobEtlBi3 = (project in file("job-etl-bi3"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= etlBi3DepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bzn-jobEtlBi3.jar"
  )

// 企业价值与个人风险
lazy val jobEtlPiwik = (project in file("job-etl-piwik"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= etlPiwikDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bzn-jobEtlPiwik.jar"
  )

// 企业价值与个人风险
lazy val jobEtlRedis = (project in file("job-etl-redis"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= etlRedisDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bzn-jobEtlRedis.jar"
  )

// 个人相关
lazy val jobPersonal = (project in file("job-personal"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= personalDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bzn-personal.jar"
  )

// error and warn
lazy val jobCountErrorAndWarn = (project in file("job-count-error"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= errorAndWarnDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bzn-count-error.jar"
  )

// crm
lazy val jobCrm = (project in file("job-crm"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= crmDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bzn-crm.jar"
  )

// c 端标签
lazy val jobCLabel = (project in file("job-c-label"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= cLabelDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bzn-jobCLabel.jar"
  )

// 和赔模型
lazy val jobPredictModel = (project in file("job-predict-model"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= predictModelDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bzn-jobPredictModel.jar"
  )

// 其他模块（没有统一的需求放在这）
lazy val jobOtherThing = (project in file("job-other-thing"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= predictModelDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bzn-jobOtherThing.jar"
  )

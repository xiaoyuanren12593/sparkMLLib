package bzn.job.until

import java.util.Date

import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FsShell, Path}
import org.apache.hadoop.hbase.client.{HTable, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait EnterpriseUntil {

  //得到企业标签数据
  def getHbaseValue(sc: SparkContext): RDD[(ImmutableBytesWritable, Result)] = {
    //定义HBase的配置
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_enterprise_vT")

    val usersRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    usersRDD
  }

  //HBaseConf 配置
  def HbaseConf(tableName: String): (Configuration, Configuration) = {
    //定义HBase的配置
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")
    conf.set("mapreduce.task.timeout", "1200000")
    conf.set("hbase.client.scanner.timeout.period", "600000")
    conf.set("hbase.rpc.timeout", "600000")
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 3000)
    //设置配置文件，为了操作hdfs文件
    val conf_fs: Configuration = new Configuration()
    conf_fs.set("fs.default.name", "hdfs://namenode1.cdh:8020")
    (conf, conf_fs)
  }

  //对文件进行权限的设置
  def proessFile(conf_fs: Configuration, stagingFolder: String): Unit = {
    val shell = new FsShell(conf_fs)
    shell.run(Array[String]("-chmod", "-R", "777", stagingFolder))

  }

  //删除HFile文件
  def deleteFile(conf_fs: Configuration, stagingFolder: String): Unit = {
    val hdfs = FileSystem.get(conf_fs)
    val path = new Path(stagingFolder)
    hdfs.delete(path)
  }

  //将hfile存到Hbase中
  def saveToHbase(result: RDD[(String, String, String)], columnFamily1: String, column: String,
                  conf_fs: Configuration, tableName: String, conf: Configuration): Unit = {
    val stagingFolder = s"/oozie/hfile/$columnFamily1/$column"
    //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
    val hdfs = FileSystem.get(conf_fs)
    val path = new Path(stagingFolder)

    //检查是否存在
    if (!hdfs.exists(path)) {
      //不存在就执行存储
      deToHbase()
    } else if (hdfs.exists(path)) {
      //存在即删除后执行存储
      deleteFile(conf_fs, stagingFolder)
      deToHbase()
    }

    def deToHbase() {
      val sourceRDD: RDD[(ImmutableBytesWritable, KeyValue)] = result
        .sortBy(_._1)
        .map(x => {
          //rowkey
          val rowKey = Bytes.toBytes(x._1)
          //列族
          val family = Bytes.toBytes(columnFamily1)
          //字段
          val colum = Bytes.toBytes(x._3)
          //当前时间
          val date = new Date().getTime
          //数据
          val value = Bytes.toBytes(x._2)

          //将RDD转换成HFile需要的格式，并且我们要以ImmutableBytesWritable的实例为key
          (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
        })

      val table = new HTable(conf, tableName)
      lazy val job = Job.getInstance(conf)
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[KeyValue])
      HFileOutputFormat.configureIncrementalLoad(job, table)
      sourceRDD.saveAsNewAPIHadoopFile(stagingFolder, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], job.getConfiguration())

      //权限设置
      proessFile(conf_fs, stagingFolder + "/*")

      //开始导入
      val bulkLoader = new LoadIncrementalHFiles(conf)
      bulkLoader.doBulkLoad(new Path(stagingFolder), table)
    }
  }

  def KeyValueToString(keyValues: Array[KeyValue], json: JSONObject, ent_name: String): String = {

    val it = keyValues.iterator
    val res = new StringBuilder
    while (it.hasNext) {
      val end = it.next()
      val row = Bytes.toString(end.getRow)
      //列族
      val family = Bytes.toString(end.getFamily)
      //字段
      val qual = Bytes.toString(end.getQualifier)
      //字段值
      val value = Bytes.toString(end.getValue)

      // res.append(row + "->" + family + "->" + qual + "->" + value + ",")
      json.put("row", row)
      json.put("family", family)
      json.put("qual", qual)
      json.put("value", value)
      res.append(json.toString + ";")
    }
    s"${ent_name}mk6${res.substring(0, res.length - 1)}"
  }
}

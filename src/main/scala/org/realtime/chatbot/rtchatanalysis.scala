package org.realtime.chatbot

import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._

object rtchatanalysis {
//  def writeToDb(df:org.apache.spark.sql.DataFrame,tblname:String):Unit={
//    // DB Connection properties
//    val prop=new java.util.Properties();
//    prop.put("user", "root")
//    prop.put("password", "root")
//    prop.put("driver","com.mysql.jdbc.Driver")
//    df.write.mode("append").jdbc("jdbc:mysql://localhost/custdb",tblname,prop)
//  }
  def main(args:Array[String])
  {
    val spark =
    SparkSession.builder.appName("chatbot").config("hive.metastore.uris","thrift://localhost:9083")
    .config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
    .config("es.nodes","localhost").config("es.port","9200")
    .config("es.index.auto.create","true").config("es.mapping.id","id")
    .enableHiveSupport.master("local[*]")
    .getOrCreate();
    
    val sc = spark.sparkContext;
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("hdfs://localhost:54310/tmp/ckpdir")
    // Create the context
    val ssc = new StreamingContext(sc, Seconds(2))
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    
    val stopwordsdf = spark.read.option("header",false).
                            csv("/home/hduser/SPARK_HACKATHON_2021/realtimedataset/stopwords").
                            toDF("stopword")
    
    stopwordsdf.createOrReplaceTempView("stopwords")
    stopwordsdf.cache()
    stopwordsdf.printSchema()
    stopwordsdf.show(5)
    println("created default.testchat db")
    spark.sql("""create external table if not exists default.testchat (id integer,chat_splits string)
          row format delimited fields terminated by ',' 
          location '/user/hduser/testchat'""")
    val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "wd24",
    "auto.offset.reset" -> "earliest"
    )                         
    // val topics = Array("clickstream")
    val topics = Array("chatbot")
    
    val stream = KafkaUtils.createDirectStream[String, String](ssc,
    PreferConsistent, // PreferBroker / PreferFixed
    Subscribe[String, String](topics, kafkaParams))
    
    import org.apache.spark.sql.functions._
    val kafkastream = stream.map(record => record.value)
    kafkastream.foreachRDD{x=>
        println("checking rdd is not empty")
        if(!x.isEmpty())
        {  
          println("iterating rdd")
          val jsondf =spark.read.option("multiline", "true").option("mode", "DROPMALFORMED").json(x)
          val chatdata = jsondf.select("id","chattext").filter("agentcustind='c'")
          chatdata.createOrReplaceTempView("chatdata")
          val splitchat = spark.sql("select id, split(chattext,' ') as chat_tokens from chatdata") 
          val explodedata = splitchat.select($"id", explode($"chat_tokens").alias("chat_splits"))
          explodedata.createOrReplaceTempView("chatwords")
          val joindata = explodedata.join(
                stopwordsdf, explodedata("chat_splits") === stopwordsdf("stopword"), "leftouter"
              ).filter("stopword is null").drop("stopword")
          joindata.createOrReplaceTempView("joindataview")
          //join(stopwordsdf, explodedata("chat_splits")==stopwordsdf("stopword"),"leftouter")
          println("Transformed data:")
          joindata.show(5)
          //writeToDb(dftodb,"stage_chatdata")
          //joindata.write.saveAsTable('')
          
          //Applying dynamic partition Hive table load in Spark
          spark.sql("""insert into table default.testchat select id,chat_splits from joindataview""")
          //or
          //joindata.write.mode("overwrite").saveAsTable("default.testchat2") // this table can't be read by hive
          println("saved to Hive")
          val aggresult = spark.sql(""" select id, chat_splits, count(chat_splits) cnt
                                        from default.testchat 
                                        group by chat_splits, id order by cnt desc""")
          aggresult.show(5)
          aggresult.saveToEs("chatidx/freqwords")
          
        }
      }
    ssc.start()
    ssc.awaitTerminationOrTimeout(300000)
   }
}
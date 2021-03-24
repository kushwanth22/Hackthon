package org.realtime.sparkcore

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

object Core{
  def f_rdd_20191001(x:RDD[Insure]):RDD[Insure]={ return x.filter(x=>x.BusinessDate=="2019-10-01") }
  def f_rdd_20191002(x:RDD[Insure]):RDD[Insure]={ return x.filter(x=>x.BusinessDate=="2019-10-02") }

  def main(args:Array[String]){
    /*val conf = new SparkConf().setAppName("SparkCore-Hackthon").setMaster("local[*]")
  	val sc = new SparkContext(conf)
  	val sqlc = new SQLContext(sc)*/
    val spark=SparkSession.builder().appName("SparkCore-Hackthon").master("local[*]")
    .config("spark.history.fs.logDirectory", "file:///tmp/spark-events")
    .config("spark.eventLog.dir", "file:///tmp/spark-events")
    .config("spark.eventLog.enabled", "true")
    .config("hive.metastore.uris","thrift://localhost:9083")
    .config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate();
    val sc = spark.sparkContext
    val sqlc = spark.sqlContext
    
    sc.setLogLevel("ERROR")
    /*println(sc)
    println(spark.sparkContext)
    println(sqlc)
    println(spark.sqlContext)*/
    val files = List("hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo1.csv",
        "hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo2.csv")
    //13: Merged insured 1 and 2  using union 
    var insuredatamerged = sc.emptyRDD[Insure]
    var insure_records_count = 0
    for(file <- files){
      //1 to 13 sol
      val (insure_data,insure_count) = reusablefunc(sc, sqlc, spark, file)
      insuredatamerged = insuredatamerged.union(insure_data)
      insure_records_count += insure_count
    }
    
    //15. Calculate the count of rdds
    if (insuredatamerged.count == insure_records_count){
      println("count Matched")
      println("Count result: " + insuredatamerged.count)
      println(insuredatamerged.first)
    } 
    else { 
      println("Count Mismatched") 
      println("insuredatamerged Count result: " + insuredatamerged.count)
      println("insure_records_count result: " + insure_records_count)
    }
    
    //14. Persist
    import org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER
    insuredatamerged.persist(MEMORY_ONLY_SER)
    
    //16. Remove duplicates 
    val drop_insuredata_duplicates = insuredatamerged.distinct()
    println(s"duplicate rows count:${insuredatamerged.count()-drop_insuredata_duplicates.count()}")
    
    //17. Increase the number of partitions in the above rdd to 8
    println(drop_insuredata_duplicates.getNumPartitions)
    val insuredatarepart = drop_insuredata_duplicates.repartition(8)
    println(s"Num of Partitions: ${insuredatarepart.getNumPartitions}")
    
    //18. Split the above RDD using the businessdate field
    val (rdd_20191001,rdd_20191002) = (f_rdd_20191001(insuredatarepart),f_rdd_20191002(insuredatarepart))
    println("20191001 Count:" + rdd_20191001.count)
    println("20191002 Count:" + rdd_20191002.count)
    
    //20. Convert the RDD created in step 17 above into Dataframe
    import spark.sqlContext.implicits._
    val insuredaterepartdf = insuredatarepart.toDF()  
    //println("schema: \n" + insuredaterepartdf.printSchema())
    //print(insuredaterepartdf.show(1))
    
    import scala.sys.process._
    val targetPath = "/user/hduser/Hackthon/coreinsure/insuredatamerged.csv,/user/hduser/Hackthon/coreinsure/insuredata_*.csv"
    
    s"hdfs dfs -rm -r -f /user/hduser/Hackthon/coreinsure/insuredatamerged.csv"! 
    
    s"hdfs dfs -rm -r -f /user/hduser/Hackthon/coreinsure/insuredata_*.csv"!
    //s"hdfs dfs -rm -rf /user/hduser/Hackthon/coreinsure/insuredata_*.csv" !
    
    //19. save 10, 13, 18
    insuredatamerged.saveAsTextFile("hdfs://localhost:54310/user/hduser/Hackthon/coreinsure/insuredatamerged.csv")
    rdd_20191001.saveAsTextFile("hdfs://localhost:54310/user/hduser/Hackthon/coreinsure/insuredata_20191001.csv")
    rdd_20191002.saveAsTextFile("hdfs://localhost:54310/user/hduser/Hackthon/coreinsure/insuredata_20191002.csv")
  }
  
   def reusablefunc(sc:SparkContext, sqlc:SQLContext, spark:SparkSession, filepath:String):(RDD[Insure],Int) = {
     //import spark.sqlContext.implicits._
     //1:
    println("Processing file:" + filepath)
    val insuredata = sc.textFile(filepath)
    //println(insuredata.first())
    //2:
    val header=insuredata.first
    val insuredata_no_header = insuredata.filter(x => x!=header)
    //println(insuredata_no_header.first())
    //3: 
    //hadoop fs -cat /user/hduser/sparkhack2/insuranceinfo1.csv|tail -2
    //val insuredata_no_footer = insuredata_no_header.filter(x=>x!="footer count is 402")
    //val res_insuredata = insuredata_no_footer
    // or
    var cnt = insuredata_no_header.count
    var skipFooterLines = 1
    val insuredata_no_footer = insuredata_no_header.zipWithIndex().filter({case (line, index) => index != (cnt - skipFooterLines)}).map({case (line, index) => line})
    val res_insuredata = insuredata_no_footer
    //4: verify if header and footer is removed
    println("Count with Header and Footer: " + insuredata.count)
    println("Count without Header and Footer: " + res_insuredata.count)
    println("Header info:")
    println(insuredata.first)
    println("Result without Header:")
    println(res_insuredata.first) 
    val testcnt = res_insuredata.count
    val testFooterLines = 1
    val verify_data = res_insuredata.zipWithIndex().filter({case (line, index) => index == (testcnt - testFooterLines)})
    println("Result after footer removed:")
    println(verify_data.first())
    //5:removes empty lines
    val trim_insuredata = res_insuredata.filter(x => x.trim() != "")
    println("Verified Count after removed empty lines:" + trim_insuredata.count)
    //or
    
    /*val trim_insuredata = res_insuredata.map(x => x.trim()).filter(x => x.split(",").length != 0)
    //(or)
    val trim_insuredata = res_insuredata.map(x => x.trim().split(",")).filter(x => x.length != 0)
    //verify lengths
    val trim_insuredata = res_insuredata.map(x => x.trim().split(",").length).foreach{println}
    // check for the empty records
    val trim_insuredata = res_insuredata.map(x => x.trim()).filter(x => x.split(",").length == 0).count
    */
    //6:
    // output of split("delimiter", -1) will gives without truncate the columns with empty strings
    //ex:-Array(21989, 21989, 2019-10-01, AK, HIOS, ODS Premier,
    //https://www.modahealth.com/ProviderSearch/faces/webpages/home.xhtml?dn=ods, 13, "", "")
    val split_insuredata = trim_insuredata.map(x=>x.split(",",-1))
    
    //val split_insuredata = trim_insuredata.map(x=>x.split(","))
    //Array(21989, 21989, 2019-10-01, AK, HIOS, ODS Premier,
    //https://www.modahealth.com/ProviderSearch/faces/webpages/home.xhtml?dn=ods, 13)
    
    //7: drop malformated data
    val dropmals_insuredata = split_insuredata.filter(x=>x.size==10)
    println("Data after dropmals: " + dropmals_insuredata.count)
    //8:
    import org.realtime.sparkcore.Insure
//    case class Insure(
//      IssuerId:Int,
//      IssuerId2:Int,
//      BusinessDate:String, //java.sql.Date,
//      StateCode:String,
//      SourceName:String,
//      NetworkName:String,
//      NetworkURL:String,
//      custnum:Int,
//      MarketCoverage:String,
//      DentalOnlyPlan:String
//    )
    //java.sql.Date.valueOf(x(2))
    val schemaed_insuredata = dropmals_insuredata.map { 
      //x => Insure(x(0).toInt,x(1).toInt,x(2),x(3),x(4),x(5),x(6),x(7).toInt,x(8),x(9))
      x => Insure(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9))
    }
    println("Schema Data Count:" + schemaed_insuredata.count)
    //9:
    println("Step 7 count:" + dropmals_insuredata.count)
    println("Step 1 count:" + insuredata.count)
    println("Total Lines removed: " + (insuredata.count - dropmals_insuredata.count))
    
    //10. Create another RDD namely rejectdata
    //from step6
    val rejectdata = split_insuredata.filter(x=>x.size!=10).map(x=>(x.size,x(0)))
    //12. drop issuerid or issuerid2 empty columned rows
    val ftinsure = schemaed_insuredata.filter{ x => !(x.IssuerId.isEmpty || x.IssuerId2.isEmpty)}
    println("After Dropping empty IssuerId and IssuerId2:" + ftinsure.count)
    val pathsplit = filepath.split("/")
    //println(pathsplit.length)
    val filenamepos = pathsplit.length - 1
    //println(pathsplit(filenamepos))
    val filename = pathsplit(filenamepos).split(".csv")(0)
    //println(filename)
    //19. save 10, 13, 18
    
    import scala.sys.process._
    val rejectPath = "/user/hduser/Hackthon/coreinsure/${filename}_rejecteddata.csv"
    if (filename == "insuranceinfo1"){
      s"hdfs dfs -rm -r -f /user/hduser/Hackthon/coreinsure/insuranceinfo1_rejecteddata.csv" !
    }
    
//    hadoop fs -rmr /user/hduser/Hackthon/coreinsure/insuranceinfo1_rejecteddata.csv
//    hadoop fs -rmr /user/hduser/Hackthon/coreinsure/insuranceinfo2_rejecteddata.csv
//    hadoop fs -rmr /user/hduser/Hackthon/coreinsure/insuredatamerged.csv
//    hadoop fs -rmr /user/hduser/Hackthon/coreinsure/insuredata_20191001.csv
//    hadoop fs -rmr /user/hduser/Hackthon/coreinsure/insuredata_20191002.csv
    
    if (!rejectdata.isEmpty()){
      rejectdata.map(r => r.productIterator.mkString(",")).saveAsTextFile(s"hdfs://localhost:54310/user/hduser/Hackthon/coreinsure/${filename}_rejecteddata.csv")
    }
    
    return (ftinsure, ftinsure.count.toInt)
   }
 }
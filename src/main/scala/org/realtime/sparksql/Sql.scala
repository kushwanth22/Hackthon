package org.realtime.sparksql

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object Sql {
  def writeToDb(df:org.apache.spark.sql.DataFrame,tblname:String):Unit={
    // DB Connection properties
    val prop=new java.util.Properties();
    prop.put("user", "root")
    prop.put("password", "root")
    prop.put("driver","com.mysql.jdbc.Driver")
    df.write.mode("append").jdbc("jdbc:mysql://localhost/custdb",tblname,prop)
  }
  def main(args:Array[String]){
    println("Project B")
    val spark=SparkSession.builder().appName("SparkSQL-Hackthon").master("local[*]")
    .config("spark.history.fs.logDirectory", "file:///tmp/spark-events")
    .config("spark.eventLog.dir", "file:///tmp/spark-events")
    .config("spark.eventLog.enabled", "true")
    .config("hive.metastore.uris","thrift://localhost:9083")
    .config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate();
    
    val sc = spark.sparkContext
    val sqlc = spark.sqlContext
    
    spark.sparkContext.setLogLevel("ERROR")
    //21:
    import org.apache.spark.sql.types._
    val struct = StructType(Array(
      StructField("IssuerId",IntegerType,true),
      StructField("IssuerId2",IntegerType,true),
      StructField("BusinessDate",DateType,true),
      StructField("StateCode", StringType, true),
      StructField("SourceName", StringType, true),
      StructField("NetworkName", StringType, true),
      StructField("NetworkURL", StringType, true),
      StructField("custnum", IntegerType, true),
      StructField("MarketCoverage", StringType, true),
      StructField("DentalOnlyPlan", StringType, true)
    ))
    //22:
    val insuranceinfo=spark.read.option("header","false").option("escape",",").
    option("mode","dropmalformed").schema(struct).
    csv("hdfs://localhost:54310/user/hduser/sparkhack2/*.csv")
    
    println(insuranceinfo.count)
    println(insuranceinfo.printSchema())
    
    //23:a
    val renamedinsure=insuranceinfo.withColumnRenamed("StateCode", "stcd").withColumnRenamed("SourceName", "srcnm")
    println(renamedinsure.printSchema())
    
    //23:b, c
    import org.apache.spark.sql.functions.{concat,col,lit,udf,max,min}
    val concatinsure = renamedinsure.
    withColumn("issueridcomposite", concat(col("IssuerId").cast("String"),col("IssuerId2").cast("String"))).
    drop("IssuerId","IssuerId2","DentalOnlyPlan")
    println(concatinsure.printSchema())
    concatinsure.show(5)
    //23:d
    import org.apache.spark.sql.functions.{current_date,current_timestamp,explode}
    val insurewithtime = concatinsure.withColumn("sysdt", current_date()).withColumn("systs", current_timestamp())
    insurewithtime.printSchema()
    insurewithtime.show(5, false)
    
    //i.
    val insurecols = insurewithtime.columns
    println("--------------")
    println("Insure cols:")
    println("--------------")
    insurecols.foreach { x => println(x) }
    //ii.
    val insuredtype = insurewithtime.dtypes.map(x=>x._2)
    println("--------------")
    println("Col Types:")
    println("--------------")
    insuredtype.foreach { x => println(x) }
    //iii.
    val insureintegers = insurewithtime.dtypes.filter(x => x._2 == "StringType").map(c => c._1)
    println("--------------")
    println("Integer Cols:")
    println("--------------")
    insureintegers.foreach { x => println(x) }
    //iv.
    val insureintdata = insurewithtime.select(insureintegers.map(col):_*)
    
    //24.
    println("Before dropping null cols " + insurewithtime.count)
    val dropnullinsure = insurewithtime.na.drop()
    
    println("After dropping null cols " + dropnullinsure.count())
    //25. 26.
    //import org.realtime.hack.allmethods
    val reusableobj = new org.realtime.hack.allmethods
    val data = reusableobj.remspecialchar("test123[]- 1,(1,2)")
    println("data " + data)
    
    val udfremspecialchardsl=udf(reusableobj.remspecialchar _)
    //27:
    //val networkname = insurewithtime.select(udfremspecialchardsl($"NetworkName"))
    val insuredf = dropnullinsure.withColumn("NetworkName",udfremspecialchardsl(col("networkname")))
    insuredf.show(5,false)
    insuredf.printSchema()
    //28:
    insuredf.write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/Hackthon/sql/insuredf.json")
    //29:
    insuredf.write.mode("overwrite").option("header","true").option("delimiter", "~").
    csv("hdfs://localhost:54310/user/hduser/Hackthon/sql/insuredf.csv")
    //30:
    insuredf.createOrReplaceTempView("insuredfview")
    spark.sql("""create external table if not exists default.hackinsuredata (BusinessDate date, 
    stcd string,srcnm string, NetworkName string, NetworkURL string, custnum integer, 
    MarketCoverage string, issueridcomposite string, sysdt date, systs timestamp)
    row format delimited fields terminated by ',' 
    location '/user/hduser/Hackthon/sql/insuredfhive'""")
    
    spark.sql("""insert overwrite TABLE default.hackinsuredata select * from insuredfview""")
    
    insuredf.write.mode("overwrite").saveAsTable("default.hackinsuredata2")
    
    println("------part B(4)----d--")
    //31:
    val custstates = sc.textFile("hdfs://localhost:54310/user/hduser/sparkhack2/custs_states.csv")
    custstates.take(5)
    
    //32:
    val custfilter = custstates.map(x=>x.split(",")).filter(x=>x.length==5)
    val statesfilter = custstates.map(x=>x.split(",")).filter(x=>x.length==2)
    
    //33:
    val custstatesdf = spark.read.csv("hdfs://localhost:54310/user/hduser/sparkhack2/custs_states.csv")
    
    //34:
    val custfilterdf = custstatesdf.
    where(col("_c0").isNotNull and col("_c1").isNotNull and col("_c2").isNotNull and col("_c3").isNotNull and col("_c4").isNotNull).
    toDF("custid","fname","lname","age","profession")
    val statesfilterdf = custstatesdf.filter(col("_c2").isNull and col("_c3").isNull 
        and col("_c4").isNull).drop("_c2","_c3","_c4").toDF("stated","statedesc")
    
    //35:
    custfilterdf.createOrReplaceTempView("custview")
    statesfilterdf.createOrReplaceTempView("statesview")
    
    //36:
    dropnullinsure.createOrReplaceTempView("insureview")
    
    //37:
    spark.udf.register("remspecialcharudf", reusableobj.remspecialchar _)
    
    //38:a, b, c, d
    spark.conf.set("spark.sql.shuffle.partitions",4)
//    val sqlinsure = spark.sql("""select BusinessDate,
//                        year(BusinessDate) as yr,
//                        dayofmonth(BusinessDate) mth,
//                        stcd,
//                        srcnm,
//                        NetworkName,
//                        remspecialcharudf(NetworkName) as cleannetworkname,
//                        NetworkURL,
//                        (case 
//                          when NetworkURL like ('https%') then 'secured'
//                          when NetworkURL like ('http%') then 'non secured' 
//                           else 'no protocol' end
//                         ) as protocal,
//                        custnum,
//                        MarketCoverage,
//                        issueridcomposite,
//                        sysdt,
//                        systs from insureview""")  
    val sqlinsure = spark.sql("""select BusinessDate,
                        year(BusinessDate) as yr,
                        dayofmonth(BusinessDate) mth,
                        stcd,
                        srcnm,
                        NetworkName,
                        remspecialcharudf(NetworkName) as cleannetworkname,
                        NetworkURL,
                        (case 
                          when NetworkURL like ('https%') then 'secured'
                          when NetworkURL like ('http%') then 'non secured' 
                           else 'no protocol' end
                         ) as protocol,
                        custnum,
                        MarketCoverage,
                        issueridcomposite,
                        sysdt,
                        systs from insureview""")  
   
    sqlinsure.show(5)                    
    
    //38:c
    //val yr_mth_df = spark.sql("select BusinessDate,year(BusinessDate) as yr,dayofmonth(BusinessDate) mth from insureview")
    //yr_mth_df.show(5)
    
    //38:d
//    val protocaldf = spark.sql("""select NetworkURL, 
//                        (case 
//                          when NetworkURL like ('https%') then 'secured'
//                          when NetworkURL like ('http%') then 'non secured' 
//                           else 'no protocol' end
//                         ) as protocal 
//                        from insureview""")
    //38.e
    /**
     * e. Display all the columns from insureview including the columns derived from
     * above a, b, c, d steps with statedesc column from statesview with age,profession
     * column from custview . Do an Inner Join of insureview with statesview using
     * stcd=stated and join insureview with custview using custnum=custid.
     * /
     */
    sqlinsure.createOrReplaceTempView("sqlinsureview")
    spark.sql("select * from sqlinsureview  ")
//    val transformdata = spark.sql("""select * from 
//                        (select * from sqlinsureview inner join statesview on stcd=stated) as joindata 
//                        join custview on custnum=custid""")
    
    val transformdata = spark.sql("""select
                          custid,
                          BusinessDate,
                          yr,
                          mth,
                          NetworkURL,
                          protocol,
                          statedesc,
                          stated,
                          age,
                          profession 
                        from 
                          (select * from sqlinsureview inner join statesview on stcd=stated) as joindata 
                          join custview on custnum=custid""")
    transformdata.show(10)
    
    //39.
    transformdata.coalesce(1).write.mode("overwrite").parquet("hdfs://localhost:54310/user/hduser/Hackthon/sql/transformedinsure.parquet")
    println("Transformated data saved to -> hdfs://localhost:54310/user/hduser/Hackthon/sql/transformedinsure")
    
//    40. Write an SQL query to identify average age, count group by statedesc, protocol,
//    profession including a seqno column added which should have running sequence
//    number partitioned based on protocol and ordered based on count descending and
//    display the profession whose second highest average age of a given state and protocol.
//    For eg.
//    Seqno,Avgage,count,statedesc,protocol,profession
//    1,48.4,10000, Alabama,http,Pilot
//    2,72.3,300, Colorado,http,Economist
//    1,48.4,3000, Atlanta,https,Health worker
//    2,72.3,2000, New Jersey,https,Economist
    transformdata.createOrReplaceTempView("transformdataview")
    val kpidf = spark.sql("""select avg(age) as avg_age,
                           count(age) as count_age,
                           statedesc,
                           protocol,    
                           profession
                           from transformdataview where group by statedesc, protocol,profession"""
                         )
    kpidf.show(10)
    //41:
    writeToDb(kpidf,"insureaggregated")
    //kpidf.write.mode("append").saveAsTable("")
    println("Saved to Mysql")
  }
}
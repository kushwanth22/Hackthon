spark-submit --jars /home/hduser/install/mysql-connector-java.jar --class org.realtime.sparksql.Sql --master yarn /home/hduser/workspacespark/Hackthon/target/Hackthon-0.0.1-SNAPSHOT.jar --driver-memory 512M --num-executors 1 --executor-cores 2 --executor-memory 1G \
--spark-shuffle-compress true \
--spark-speculation true \
--spark-dynamicAllocation-enabled true \
--spark-dynamicAllocation-initialExecutors 1 \
--spark-dynamicAllocation-minExecutors 1 \
--spark-dynamicAllocation-maxExecutors 4 \
--spark-dynamicAllocation-executorIdleTimeout 30s \
--conf spark.shuffle.service.enabled true

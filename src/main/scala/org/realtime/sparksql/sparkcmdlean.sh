spark-submit --jars /home/hduser/install/mysql-connector-java.jar --class org.realtime.sparksql.Sql --master yarn /home/hduser/workspacespark/Hackthon/target/Hackthon-0.0.1-SNAPSHOT.jar --driver-memory 512M --num-executors 4 --executor-cores 2 --executor-memory 1G


# Bigdata
Bigdata

作业目录:  /home/student1/hao

作业1：
hadoop fs -put HTTP_20130313143750.dat /hao
hadoop jar Bigdata-1.0-SNAPSHOT.jar org.geekbang.time.week1.mapreduce.PhoneStatistics HTTP_20130313143750.dat /hao/HTTP_20130313143750.dat /hao/output
hadoop fs -cat /hao/output/part-r-00000

作业2：
java -cp /usr/lib/hbase-current/lib/*:Bigdata-1.0-SNAPSHOT.jar:slf4j-api-1.7.30.jar:htrace-core4-4.2.0-incubating.jar org.geekbang.time.week1.hbase.HbaseDemo


第三周作业:

Bigdata/src/main/java/org/geekbang/time/week3/


第五周作业:

Bigdata/src/main/java/org/geekbang/time/week6/

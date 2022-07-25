package io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WriteKafka {

    String desPath = "/data/testkafka";
    private SparkSession spark;
    public WriteKafka() {
        // TODO Auto-generated constructor stub
    }
    public void writeToHDFS() {
        ReadKafka read = new ReadKafka(spark);
        Dataset<Row> df = read.read();
        df.write().format("parquet").mode("append").save(desPath);
    }
    public void run() {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Kafka Integration using Structured Streaming")
                .master("local")
                .getOrCreate();

//            writeToMysql();
        writeToHDFS();
    }
    public static void main(String[] args) {
        WriteKafka write = new WriteKafka();

        write.run();
    }
}

package test;

import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

public class Test2 {

    public Test2() {
        // TODO Auto-generated constructor stub
    }
    public static void main(String[] args) {
        // Define a Spark Session
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Kafka Integration using Structured Streaming")
                .master("local")
                .getOrCreate();
        String hosts = "10.3.68.20:9092,"
                + "10.3.68.21:9092,"
                + "10.3.68.23:9092,"
                + "10.3.68.26:9092,"
                + "10.3.68.28:9092,"
                + "10.3.68.32:9092,"
                + "10.3.68.47:9092,"
                + "10.3.68.48:9092,"
                + "10.3.68.50:9092,"
                + "10.3.68.52:9092";
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", hosts)
                .option("subscribe", "rt-adn-sp")
                .load();
        df.printSchema();
    }
}

package io;

import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

public class WriteKafka {

    String desPath = "/data/testkafka";
    private SparkSession spark;
    public WriteKafka() {
        // TODO Auto-generated constructor stub
    }
    public void writeToHDFS() {
        ReadKafka read = new ReadKafka(spark);
        Dataset<Row> df = read.read().toDF();
        try {
            df.writeStream().trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS)).format("console").start(desPath).awaitTermination();
        } catch (StreamingQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        };
    }
    public void run() {
        spark = SparkSession
                .builder()
                .master("yarn-client")
                .appName("Read write data")
                .getOrCreate();
        writeToHDFS();
    }
    public static void main(String[] args) {
        WriteKafka write = new WriteKafka();

        write.run();
    }
}


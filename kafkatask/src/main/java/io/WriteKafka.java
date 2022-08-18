package io;

import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.streaming.StreamingQueryException;
public class WriteKafka {
    /**
     * Địa chỉ thư mục lưu dữ liệu.
     */
    private String desPath = "/data/nguyenlt";
    /**
     * SparkSession.
     */
    private SparkSession spark;
    /**
     * Constructor.
     */
    public WriteKafka() {
        // TODO Auto-generated constructor stub
    }
    /**
     * Ghi dữ liệu vào hdfs.
     */
    public void writeToHDFS() {
        ReadKafka read = new ReadKafka(spark);
        final long triggerTime = 20;
        Dataset<Row> df = read.read().toDF();
        try {
            df.writeStream()
                .trigger(Trigger
                        .ProcessingTime(triggerTime, TimeUnit.SECONDS))
                .format("parquet")
                .option("checkpointLocation", desPath)
                .outputMode("append")
                .start(desPath).awaitTermination();
        } catch (StreamingQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /**
     * Chạy.
     */
    public void run() {
        spark = SparkSession
                .builder()
                .appName("Read write data")
                .master("yarn")
                .getOrCreate();
        writeToHDFS();
    }
    /**
     * Hàm chính.
     * @param args
     */
    public static void main(final String[] args) {
        WriteKafka write = new WriteKafka();

        write.run();
    }
}

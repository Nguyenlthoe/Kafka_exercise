package io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadKafka {

    private final String hosts = "10.3.68.20:9092,"
            + "10.3.68.21:9092,"
            + "10.3.68.23:9092,"
            + "10.3.68.26:9092,"
            + "10.3.68.28:9092,"
            + "10.3.68.32:9092,"
            + "10.3.68.47:9092,"
            + "10.3.68.48:9092,"
            + "10.3.68.50:9092,"
            + "10.3.68.52:9092";

    /**
     * Topic.
     */
    private final String topic = "rt-adn-sp";

    /**
     * SparkSession.
     */
    private SparkSession spark;

    /**
     * Contructor.
     * @param spark
     */
    public ReadKafka(SparkSession spark) {
        this.spark = spark;
    }
    
    public Dataset<Row> read() {
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", hosts)
                .option("subscribe", topic)
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr("CAST(value AS STRING) AS value");
        return df;
    }
}

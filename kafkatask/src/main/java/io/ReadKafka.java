package io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.DateType;

public class ReadKafka {
    /**
     * địa chỉ máy chủ kafka.
     */
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
     * @param spark1
     */
    public ReadKafka(final SparkSession spark1) {
        this.spark = spark1;
    }
    /**
     * Đọc dữ liệu từ các server kafka.
     * @return dataframe
     */
    public Dataset<Row> read() {
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", hosts)
                .option("subscribe", topic)
                //.option("failOnDataLoss", "false")
                //.option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING) AS value");
        df = df.select(split(col("value"), "\t").as("split"));
        final int iBanner = 4;
        final int iGUIDTime = 3;
        final int iViewCount = 5;
        final int iGUID = 6;
        final int iDomain = 7;
        final int iZoneId = 10;
        final int iCampain = 11;
        final int iPrice = 17;
        final int iCov = 9;
        Dataset<Row> finalDf = df.select(
                col("split").getItem(0).cast(LongType)
                .cast(TimestampType).as("TimeNow"),
                col("split").getItem(1)
                .cast(LongType).as("ip"),
                col("split").getItem(2)
                .cast(StringType).as("userAgent"),
                col("split").getItem(iGUIDTime).cast(LongType)
                .cast(TimestampType).cast(DateType).as("Date"),
                col("split").getItem(iBanner)
                .cast(LongType).as("bannerId"),
                col("split").getItem(iViewCount)
                .cast(LongType).as("Viewcount"),
                col("split").getItem(iGUID)
                .cast(LongType).as("GUID"),
                col("split").getItem(iDomain)
                .cast(StringType).as("admDomain"),
                col("split").getItem(iCov)
                .cast(IntegerType).as("Cov"),
                col("split").getItem(iZoneId)
                .cast(LongType).as("ZoneId"),
                col("split").getItem(iCampain)
                .cast(LongType).as("Campain"),
                col("split").getItem(iPrice)
                .cast(LongType).as("Price"));
        System.out.println("---------------------------------------------");
        System.out.println("---------------------------------------------");
        df.printSchema();
        System.out.println("---------------------------------------------");
        System.out.println("---------------------------------------------");
        return finalDf;
    }
}


package io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ReadHdfs {
    /**
     * spark session.
     */
    private SparkSession spark;
    /**
     * pathtodata.
     */
    private final String path = "/data/nguyenlt/";
    /**
     * constructor.
     * @param spSession
     */
    public ReadHdfs(final SparkSession spSession) {
        // TODO Auto-generated constructor stub
        this.spark = spSession;
    }
    /**
     * đọc dataframe.
     * @return dataframe
     */
    public Dataset<Row> read() {
        StructType sch =  DataTypes.createStructType(new StructField[] {
                DataTypes
                .createStructField("TimeNow", DataTypes.TimestampType, true),
                DataTypes
                .createStructField("ip", DataTypes.LongType, true),
                DataTypes
                .createStructField("userAgent", DataTypes.StringType, true),
                DataTypes
                .createStructField("Date", DataTypes.DateType, true),
                DataTypes
                .createStructField("bannerId", DataTypes.LongType, true),
                DataTypes
                .createStructField("Viewcount", DataTypes.LongType, true),
                DataTypes
                .createStructField("GUID", DataTypes.LongType, true),
                DataTypes
                .createStructField("admDomain", DataTypes.StringType, true),
                DataTypes
                .createStructField("Cov", DataTypes.IntegerType, true),
                DataTypes
                .createStructField("ZoneId", DataTypes.LongType, true),
                DataTypes
                .createStructField("Campain", DataTypes.LongType, true),
                DataTypes
                .createStructField("Price", DataTypes.LongType, true),
        });
        Dataset<Row> df = spark.readStream().schema(sch)
                .format("parquet").load(path);
        return df;
    }
}

package io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
        Dataset<Row> df = spark.readStream().format("parquet").load(path);
        return df;
    }
}

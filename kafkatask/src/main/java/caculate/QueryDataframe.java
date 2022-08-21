package caculate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.col;
public final class QueryDataframe {
    /**
     * constructor.
     */
    public QueryDataframe() {
        // TODO Auto-generated constructor stub
    }
    /**
     * Tính số lượng click và view của một campain.
     * @param df
     */
    public void countClickAndView(final Dataset<Row> df) {
        Dataset<Row> joinDf = df.filter(col("Cov").equalTo("0"))
                .groupBy(col("Campain")).count().as("view").join(df.filter(col("Cov").equalTo("1"))
                .groupBy(col("Campain")).count().as("click"),
                df.filter(col("Cov").equalTo("0"))
                .groupBy(col("Campain")).count().as("view").col("Campain").equalTo(df.filter(col("Cov").equalTo("1"))
                        .groupBy(col("Campain")).count().as("click").col("Campain")), "full");
        joinDf = joinDf.na().fill(0);
        joinDf.printSchema();
        try {
            joinDf.writeStream().format("console")
            .start().awaitTermination();
        } catch (StreamingQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

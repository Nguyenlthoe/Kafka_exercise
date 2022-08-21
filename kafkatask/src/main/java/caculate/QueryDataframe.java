package caculate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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
        Dataset<Row> clickDf = df.filter(col("Cov").equalTo("1"))
                .groupBy(col("Campain")).count().as("click");
        Dataset<Row> viewDf = df.filter(col("Cov").equalTo("0"))
                .groupBy(col("Campain")).count().as("view");
        Dataset<Row> joinDf = viewDf.join(clickDf,
                viewDf.col("Campain").equalTo(clickDf.col("Campain")), "full");
        joinDf = joinDf.na().fill(0);
        joinDf.printSchema();
        joinDf.write();
    }
}

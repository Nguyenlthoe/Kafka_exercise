package caculate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

import scala.collection.JavaConverters;
import static org.apache.spark.sql.functions.countDistinct;
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
        final int numberRow = 30;
        Dataset<Row> clickDf = df.filter(col("Cov").equalTo("1"))
                .groupBy(col("Campain")).count().as("clickDf");
        clickDf = clickDf.select(col("Campain"),
                col("count").as("click"));
        Dataset<Row> viewDf = df.filter(col("Cov").equalTo("0"))
                .groupBy(col("Campain")).count().as("viewDf");
        viewDf = viewDf.select(col("Campain"), col("count").as("view"));
        List<String> campainString = new ArrayList<String>();
        campainString.add("Campain");
        Dataset<Row> joinDf = viewDf.join(clickDf, JavaConverters
                .asScalaIteratorConverter(campainString.iterator())
                .asScala().toSeq(), "outer");
        joinDf = joinDf.na().fill(0);
        joinDf.show(numberRow);
    }
    /**
     * Tính số lượng location của 1 campain.
     * @param df
     */
    public void countLocation(final Dataset<Row> df) {
        final int numberRow = 30;
        Dataset<Row> locationDf = df
                .select(col("Campain"), col("ZoneId"));
        locationDf = locationDf.groupBy(col("Campain"))
                .agg(countDistinct(col("ZoneId")).as("number Of Location"));
        locationDf.show(numberRow);
    }
    /**
     * Tính số lượng user của từng campain.
     * @param df
     */
    public void countUser(final Dataset<Row> df) {
        final int numberRow = 30;
        Dataset<Row> userDf = df
                .select(col("Campain"), col("GUID"));
        userDf = userDf.groupBy(col("Campain"))
                .agg(countDistinct(col("GUID")).as("number Of User"));
        userDf.show(numberRow);
    }
    /** Tính số lượng user vào nhiều campain.
     * @param df
     * @param campains
     */
    public void countUserOfCampains(final Dataset<Row> df,
            final ArrayList<String> campains) {
        String condition = "";
        if (campains.size() > 0) {
            condition = "Campain = " + campains.get(0);
        }
        for (int i = 1; i < campains.size(); i++) {
            condition = condition + " or Campain = " + campains.get(i);
        }
        Dataset<Row> userDf = df
                .select(col("Campain"), col("GUID"));
        userDf = userDf.filter(condition);
        userDf = userDf.groupBy(col("GUID"))
                .agg(countDistinct(col("Campain"))
                        .as("numberOfCampain"));
        userDf = userDf.filter("numberOfCampain = " + campains.size());
        long count = userDf.count();
        System.out.println("----------------------------------------------");
        System.out.println(condition + "\n");
        System.out.print("Number User: " + count + "\n");
        System.out.println("----------------------------------------------");
    }
}

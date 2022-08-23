package todo;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import caculate.QueryDataframe;
import io.ReadHdfs;

public final class CountUserOfCampains {
    /**
     * constructor.
     */
    private CountUserOfCampains() {
        // TODO Auto-generated constructor stub
    }
    /**
     * main.
     * @param args
     */
    public static void main(final String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Query hdfs")
                .master("yarn")
                .getOrCreate();
        ReadHdfs read = new ReadHdfs(spark);
        ArrayList<String> campains = new ArrayList<String>(Arrays.asList(args));
        Dataset<Row> df = read.read();
        QueryDataframe queryDf = new QueryDataframe();
        queryDf.countUserOfCampains(df, campains);

    }

}

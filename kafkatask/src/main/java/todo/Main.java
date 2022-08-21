package todo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import caculate.QueryDataframe;
import io.ReadHdfs;

public final class Main {
    /**
     * constructor.
     */
    private Main() {
        // TODO Auto-generated constructor stub
    }
    /**
     * main.
     * @param args
     */
    public static void main(final String[] args) {
        final long timeUpdate = 60000;
        SparkSession spark = SparkSession.builder()
                .appName("Query hdfs")
                .master("yarn")
                .getOrCreate();
        while (true) {
            ReadHdfs read = new ReadHdfs(spark);
            Dataset<Row> df = read.read();
            QueryDataframe queryDf = new QueryDataframe();
            queryDf.countClickAndView(df);
            try {
                Thread.sleep(timeUpdate);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}

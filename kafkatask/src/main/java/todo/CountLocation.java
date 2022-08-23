package todo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import caculate.QueryDataframe;
import io.ReadHdfs;

public final class CountLocation {
    /**
     * constructor.
     */
    private CountLocation() {
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
//            ArrayList<String> campains = new ArrayList<String>();
//            campains.add("203141");
//            campains.add("203611");
            Dataset<Row> df = read.read();
            QueryDataframe queryDf = new QueryDataframe();
            //queryDf.countUser(df);
            queryDf.countLocation(df);
            try {
                Thread.sleep(timeUpdate);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}

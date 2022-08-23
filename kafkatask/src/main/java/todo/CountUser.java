package todo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import caculate.QueryDataframe;
import io.ReadHdfs;

public final class CountUser {
    /**
     * constructor.
     */
    private CountUser() {
        // TODO Auto-generated constructor stub
    }
    /**
     * Tính toán số lượng user cho từng campains.
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
            Dataset<Row> df = read.read();
            QueryDataframe queryDf = new QueryDataframe();
            queryDf.countUser(df);
            try {
                Thread.sleep(timeUpdate);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}

package reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SalaryReader {
    public static Dataset<Row> read(SparkSession spark, String path) {
        return spark.read().option("header", "true").csv(path);
    }
}

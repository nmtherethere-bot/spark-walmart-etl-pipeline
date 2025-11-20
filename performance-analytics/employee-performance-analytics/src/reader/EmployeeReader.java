package reader;
import org.apache.spark.sql.*;
public class EmployeeReader {
    public static Dataset<Row> read(SparkSession spark, String path) {
        return spark.read().option("header", "true").csv(path);
    }
}

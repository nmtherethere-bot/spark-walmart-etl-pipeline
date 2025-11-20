package writer;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class FinalWriter {

    public static void write(Dataset<Row> df, String outputPath) {
        df.write().mode("overwrite").parquet(outputPath);
    }
}


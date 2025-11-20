package processor;



import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class AttendanceProcessor {

    public static Dataset<Row> calculateAttendance(Dataset<Row> att) {

        Dataset<Row> cleaned = att.withColumn(
                "present", when(col("status").equalTo("P"), 1).otherwise(0)
        );

        return cleaned.groupBy("emp_id")
                .agg(
                        sum("present").alias("total_present"),
                        count("*").alias("total_days")
                );
    }
}


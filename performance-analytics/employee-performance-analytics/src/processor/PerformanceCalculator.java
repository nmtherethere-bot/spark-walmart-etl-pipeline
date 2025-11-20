package processor;



import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class PerformanceCalculator {

    public static Dataset<Row> calculate(Dataset<Row> emp,
                                         Dataset<Row> attendance,
                                         Dataset<Row> salary) {

        Dataset<Row> joined = emp
                .join(attendance, "emp_id")
                .join(salary, "emp_id");

        return joined.withColumn("attendance_pct",
                        col("total_present").multiply(100).divide(col("total_days"))
                )
                .withColumn("performance_score",
                        col("attendance_pct").multiply(0.7)
                );
    }
}


package com.example.walmart.etl.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class SalesTransform {

    public static Dataset<Row> transform(Dataset<Row> df, String dateFormat) {
        Dataset<Row> withCasts = df
                .withColumn("order_date", to_date(col("order_date"), dateFormat))
                .withColumn("unit_price", col("unit_price").cast("decimal(10,2)"))
                .withColumn("quantity", col("quantity").cast("int"))
                .withColumn("category", upper(trim(col("category"))))
                .withColumn("product_name", trim(col("product_name")))
                .withColumn("store_id", trim(col("store_id")))
                .withColumn("order_id", trim(col("order_id")));

        Dataset<Row> filtered = withCasts
                .filter(col("order_id").isNotNull())
                .filter(col("order_date").isNotNull())
                .filter(col("unit_price").geq(lit(0)))
                .filter(col("quantity").gt(0));

        Dataset<Row> enriched = filtered
                .withColumn("gross_sales", expr("CAST(unit_price * quantity AS DECIMAL(12,2))").cast("double"))
                .withColumn("order_year", year(col("order_date")))
                .withColumn("order_month", month(col("order_date")));

        return enriched;
    }

    public static Dataset<Row> transform(Dataset<Row> df) {
        return transform(df, "yyyy-MM-dd");
    }
}

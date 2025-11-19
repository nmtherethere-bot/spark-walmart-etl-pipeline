package com.example.walmart.etl;

import com.example.walmart.etl.transform.SalesTransform;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.functions;

import java.util.HashMap;
import java.util.Map;

public class ETLJob {

    public static void main(String[] args) {
        Map<String, String> params = parseArgs(args);

        String inputPath = required(params, "inputPath");
        String mode = params.getOrDefault("mode", "append");
        String dateFormat = params.getOrDefault("dateFormat", "yyyy-MM-dd");

        // Dry-run mode: if --mode=dryrun, skip BigQuery params and output only previews
        boolean dryrun = "dryrun".equalsIgnoreCase(mode);
        String bqProject = dryrun ? null : required(params, "bqProject");
        String bqDataset = dryrun ? null : required(params, "bqDataset");
        String bqTable   = dryrun ? null : required(params, "bqTable");
        String tempBucket = dryrun ? null : required(params, "tempGcsBucket");

        SparkSession spark = SparkSession.builder()
                .appName("WalmartSalesETL")
                .master(params.getOrDefault("master", "local[*]"))
                .getOrCreate();

        Dataset<Row> raw = spark.read()
                .option("header", "true")
                .option("mode", "PERMISSIVE")
                .csv(inputPath);

        Dataset<Row> input = raw
                .withColumnRenamed("invoice_id", "order_id")
                .withColumnRenamed("date", "order_date")
                .withColumnRenamed("product_line", "category")
                .withColumnRenamed("branch", "store_id")
                // FIXED: using 'category' instead of the removed 'product_line'
                .withColumn("product_name", functions.col("category"));

        Dataset<Row> transformed = SalesTransform.transform(input, dateFormat);

        if (dryrun) {
            transformed.show(20, false);
            System.out.println("Transformed rows: " + transformed.count());

            String outPath = params.get("outPath");
            if (outPath != null && !outPath.isEmpty()) {
                transformed.coalesce(1).write().mode("overwrite").parquet(outPath);
                System.out.println("Wrote preview to " + outPath);
            }
        } else {
            String fullTable = String.format("%s.%s.%s", bqProject, bqDataset, bqTable);
            transformed.write()
                    .format("bigquery")
                    .option("table", fullTable)
                    .option("temporaryGcsBucket", tempBucket)
                    .mode(mode)
                    .save();
        }

        spark.stop();
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        for (String a : args) {
            if (a.startsWith("--") && a.contains("=")) {
                int i = a.indexOf('=');
                String k = a.substring(2, i);
                String v = a.substring(i + 1);
                map.put(k, v);
            }
        }
        return map;
    }

    private static String required(Map<String, String> params, String key) {
        String v = params.get(key);
        if (v == null || v.isEmpty()) {
            throw new IllegalArgumentException("Missing required param: " + key);
        }
        return v;
    }
}

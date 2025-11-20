package com.example.performance;

import org.apache.spark.sql.*;
import processor.AttendanceProcessor;
import processor.PerformanceCalculator;
import reader.AttendanceReader;
import reader.EmployeeReader;
import reader.SalaryReader;
import writer.FinalWriter;

import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) {

        try {

            // ------------------------------
            // Parse CLI arguments
            // ------------------------------
            Map<String, String> params = parseArgs(args);
            int uiWaitSeconds = Integer.parseInt(params.getOrDefault("uiWaitSeconds", "120"));
            String basePath = params.getOrDefault("basePath", "/data/");
            String outputPath = params.getOrDefault("outputPath", "/output/employee_performance");

            // ------------------------------
            // Create Spark Session
            // ------------------------------
            SparkSession spark = SparkSession.builder()
                    .appName("EmployeePerformanceAnalytics")
                    .master("local[*]") // override with --master if needed
                    .config("spark.ui.enabled", "true")
                    .config("spark.sql.shuffle.partitions", "10")
                    .getOrCreate();

            // ------------------------------
            // Read Input CSV Files
            // ------------------------------
            Dataset<Row> emp = EmployeeReader.read(spark, basePath + "employees.csv");
            Dataset<Row> att = AttendanceReader.read(spark, basePath + "attendance.csv");
            Dataset<Row> sal = SalaryReader.read(spark, basePath + "salaries.csv");

            // ------------------------------
            // Process & Aggregate Data
            // ------------------------------
            Dataset<Row> attendanceAgg = AttendanceProcessor.calculateAttendance(att);
            Dataset<Row> finalDf = PerformanceCalculator.calculate(emp, attendanceAgg, sal);

            // ------------------------------
            // Write Output
            // ------------------------------
            FinalWriter.write(finalDf, outputPath);

            // ------------------------------
            // Keep Spark UI Alive
            // ------------------------------
            if (uiWaitSeconds > 0) {
                System.out.println("Keeping Spark UI alive for " + uiWaitSeconds + " seconds...");
                Thread.sleep(uiWaitSeconds * 1000L);
            }

            spark.stop();
            System.out.println("Spark job completed.");

        } catch (Exception e) {
            System.err.println("❌ ERROR in Main Job: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ------------------------------
    // Argument Parser --key=value
    // ------------------------------
    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        if (args != null) {
            for (String a : args) {
                if (a.startsWith("--") && a.contains("=")) {
                    String[] parts = a.substring(2).split("=", 2);
                    map.put(parts[0], parts[1]);
                }
            }
        }
        return map;
    }
}

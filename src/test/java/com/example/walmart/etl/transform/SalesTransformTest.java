package com.example.walmart.etl.transform;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SalesTransformTest {

    private static SparkSession spark;

    @BeforeAll
    static void setup() {
        spark = SparkSession.builder()
                .appName("SalesTransformTest")
                .master("local[1]")
                .config("spark.sql.codegen.wholeStage", "false")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
    }

    @AfterAll
    static void teardown() {
        if (spark != null) spark.stop();
    }

    @Test
    void transformsAndFiltersCorrectly() {
        StructType schema = new StructType()
                .add("order_id", DataTypes.StringType, false)
                .add("order_date", DataTypes.StringType, false)
                .add("store_id", DataTypes.StringType, true)
                .add("product_id", DataTypes.StringType, true)
                .add("product_name", DataTypes.StringType, true)
                .add("category", DataTypes.StringType, true)
                .add("unit_price", DataTypes.StringType, true)
                .add("quantity", DataTypes.StringType, true);

        List<Row> rows = Arrays.asList(
                RowFactory.create("O1", "2024-08-01", "S001", "P001", "Widget", "home", "10.50", "3"),
                RowFactory.create("O2", "2024-08-02", "S001", "P002", "Gadget", "electronics", "0.00", "0"),
                RowFactory.create(null, "2024-08-03", "S002", "P003", "Thing", "toys", "5.00", "2")
        );

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        Dataset<Row> out = SalesTransform.transform(df);

        Row r = out.filter("order_id = 'O1'").head();
        assertEquals("O1", r.<String>getAs("order_id"));
        assertEquals(2024, r.<Integer>getAs("order_year"));
        assertEquals(8, r.<Integer>getAs("order_month"));
        assertEquals(31.5, r.<Double>getAs("gross_sales"));
        assertEquals("HOME", r.<String>getAs("category"));
    }
}

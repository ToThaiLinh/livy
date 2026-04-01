package com.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;

public class SimpleJob {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Simple Spark Job Java")
                .getOrCreate();

        // Tạo DataFrame
        Dataset<Row> df = spark.createDataFrame(
                Arrays.asList(
                        new Person(1, "Alice"),
                        new Person(2, "Bob"),
                        new Person(3, "Charlie")
                ),
                Person.class
        );

        // Xử lý
        Dataset<Row> result = df.filter(col("id").gt(1));

        result.show();

        spark.stop();
    }
}
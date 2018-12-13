package spark;

import com.arangodb.spark.ArangoSpark;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Collection;
import java.util.Collections;


class KafkaSpark {
    KafkaSpark(){

    }


    void startStream() throws StreamingQueryException {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaSparkStreaming")
                .set("arangodb.hosts", "127.0.0.1:8529")
                .set("arangodb.user", "root")
                .set("arangodb.password", "arangodebe");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, new Duration(1000));

        Collection<String> topics = Collections.singletonList("sparkStream");


        ObjectMapper objectMapper = new ObjectMapper();

        SQLContext sqlContext = new SQLContext(sparkContext);

        Dataset<Row> df = sqlContext.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("startingOffsets","earliest")
                .option("subscribe", "sparkStream")
                .load();


        StructType book = new StructType()
                .add("title", DataTypes.StringType)
                .add("author", DataTypes.StringType)
                .add("publisher", DataTypes.StringType);

        Dataset<Row> result = df.selectExpr("CAST(value AS STRING)").select("value");
        Dataset<Row> toBook = result.select(functions.from_json(
                result.col("value"),book)
                .as("book")
        );

        //select column
        Dataset<Row> filteredBook = toBook.selectExpr("book.title","book.author");


        StreamingQuery squery = filteredBook.writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (rowDataset, aLong) -> {
                    rowDataset.write()
                            .mode("append")
                            .parquet("hdfs://localhost/datastore/blablabla-1.parquet");
                    ArangoSpark.save(rowDataset, "sparkDummy");
                })
                .start();
        squery.awaitTermination();

    }

}

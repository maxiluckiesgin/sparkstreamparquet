package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;

import java.util.Iterator;
import java.util.List;


class KafkaSpark {
    KafkaSpark() {

    }


    void startStream() throws StreamingQueryException {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaSparkStreaming")
                .set("arangodb.hosts", "127.0.0.1:8529")
                .set("arangodb.user", "root")
                .set("arangodb.password", "arangodebe");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sparkContext);

        Dataset<Row> df = sqlContext.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("startingOffsets", "latest")
                .option("subscribe", "sparkStream")
                .load();


        StructType book = new StructType();

        Dataset<Row> result = df.selectExpr("CAST(value AS STRING)").select("value");


        Dataset<Row> toBook = result.select(functions.from_json(
                result.col("value"), book)
                .as("book")
        );

        //select column
        Dataset<Row> filteredBook = toBook.selectExpr("*");


        DataStreamWriter<Row> bookQuery = filteredBook.writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (rowDataset, aLong) -> {
//                    squery.stop();
                    if(!rowDataset.isEmpty()){
                        System.out.println("using latest");
                        rowDataset.show();
//                        rowDataset.write()
//                                .mode("append")
//                                .parquet("hdfs://localhost/datastore/blablabla-1.parquet");
//                        ArangoSpark.save(rowDataset, "sparkDummy");
                    }

                });


        StreamingQuery squery = result.writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (rowDataset, aLong) -> {
                    if (!rowDataset.isEmpty()) {
                        List<String> listOne = rowDataset.as(Encoders.STRING()).collectAsList();

                        System.out.println(listOne.get(0));

                        JSONObject jsonObject = new JSONObject(listOne.get(0).trim());

                        Iterator<String> keys = jsonObject.keys();

                        while (keys.hasNext()) {
                            String key = keys.next();
                            book.add(key, DataTypes.StringType);
                        }

                        bookQuery.start().awaitTermination();

                    }

//
                })
                .start();




        squery.awaitTermination();






    }

}

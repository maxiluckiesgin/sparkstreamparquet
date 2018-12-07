import java.util.*;

import org.apache.spark.sql.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;



public class Application{



     public static void main(String[] args) throws InterruptedException {

         Map<String, Object> kafkaParams = new HashMap<>();



         kafkaParams.put("bootstrap.servers", "localhost:9092");
         kafkaParams.put("key.deserializer", StringDeserializer.class);
         kafkaParams.put("value.deserializer", StringDeserializer.class);
         kafkaParams.put("group.id", "firstGroup");
         kafkaParams.put("auto.offset.reset", "latest");
         kafkaParams.put("enable.auto.commit", false);

         Collection<String> topics = Arrays.asList("sparkStream");

         SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");


         JavaSparkContext sparkContext = new JavaSparkContext(conf);

         JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, new Duration(1000));

         JavaInputDStream<ConsumerRecord<String, String>> stream =
                 KafkaUtils.createDirectStream(
                         streamingContext,
                         LocationStrategies.PreferConsistent(),
                         ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                 );

         stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

         stream.foreachRDD(
                 javaRDD ->{

                     JavaRDD<Message> map = javaRDD.map(
                             consumerRecord -> {
                                 Message m = new Message();
                                 m.setValue(consumerRecord.value());
                                 return m;
                             }
                     );


                     SQLContext sqlContext = new SQLContext(sparkContext);

                     sqlContext.setConf("spark.sql.parquet.compression.codec","snappy");


                     Dataset<Row> mDF = sqlContext.createDataFrame(map, Message.class);


                     if(!mDF.isEmpty()){

                         mDF.show();

                         mDF.write().parquet("blablabla.parquet");

                     }


                 }

         );

         streamingContext.start();
         streamingContext.awaitTermination();

     }


}

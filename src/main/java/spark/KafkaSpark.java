package spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.annotation.Bean;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;



class KafkaSpark {
    private Map<String, Object> kafkaParams = new HashMap<>();

    private JavaSparkContext sparkContext;

    private JavaStreamingContext streamingContext;

    private JavaInputDStream<ConsumerRecord<String, String>> stream;



    KafkaSpark(){


        this.kafkaParams.put("bootstrap.servers", "localhost:9092");
        this.kafkaParams.put("key.deserializer", StringDeserializer.class);
        this.kafkaParams.put("value.deserializer", StringDeserializer.class);
        this.kafkaParams.put("key.serializer", StringSerializer.class);
        this.kafkaParams.put("value.serializer", StringSerializer.class);
        this.kafkaParams.put("group.id", "firstGroup");
        this.kafkaParams.put("auto.offset.reset", "latest");
        this.kafkaParams.put("enable.auto.commit", false);

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        this.sparkContext = new JavaSparkContext(conf);

        this.streamingContext = new JavaStreamingContext(sparkContext, new Duration(1000));

        Collection<String> topics = Collections.singletonList("sparkStream");

        this.stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

    }

    void startStream() throws InterruptedException {

        System.out.println("starting steam");


        ObjectMapper objectMapper = new ObjectMapper();

        this.stream.foreachRDD(
                javaRDD ->{

                    JavaRDD<Message> map = javaRDD.map(
                            consumerRecord -> objectMapper.readValue(consumerRecord.value(),Message.class)
                    );


                    SQLContext sqlContext = new SQLContext(this.sparkContext);


                    Dataset<Row> mDF = sqlContext.createDataFrame(map, Message.class);


                    if(!mDF.isEmpty()){

                        mDF.show();

                        mDF.write().mode("overwrite").parquet("blablabla.parquet");

                    }


                }

        );

        this.streamingContext.start();
        this.streamingContext.awaitTermination();

    }

}

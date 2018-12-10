package spark;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;

import java.util.HashMap;
import java.util.Map;

class KafkaProducer {

    private Map<String, Object> kafkaParams = new HashMap<>();


    KafkaProducer(){


        this.kafkaParams.put("bootstrap.servers", "localhost:9092");
        this.kafkaParams.put("key.deserializer", StringDeserializer.class);
        this.kafkaParams.put("value.deserializer", StringDeserializer.class);
        this.kafkaParams.put("key.serializer", StringSerializer.class);
        this.kafkaParams.put("value.serializer", StringSerializer.class);
        this.kafkaParams.put("group.id", "firstGroup");
        this.kafkaParams.put("auto.offset.reset", "latest");
        this.kafkaParams.put("enable.auto.commit", false);

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
    }

    ProducerRecord<String, String> producerRecord(String topic, String message){
        return new ProducerRecord<>(topic, message);
    }


    Producer<String, String> kafkaProducer() {
        return new org.apache.kafka.clients.producer.KafkaProducer<>(kafkaParams);
    }
}

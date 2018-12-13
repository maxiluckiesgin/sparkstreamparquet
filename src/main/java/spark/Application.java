package spark;

import org.apache.spark.sql.streaming.StreamingQueryException;

public class Application{


     public static void main(String[] args) throws StreamingQueryException {
         spark.KafkaSpark kafkaSpark = new spark.KafkaSpark();
         kafkaSpark.startStream();
     }


}

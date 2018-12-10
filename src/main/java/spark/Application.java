package spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class Application{


     public static void main(String[] args) {
//         spark.KafkaSpark kafkaSpark = new spark.KafkaSpark();
//
//         kafkaSpark.startStream();
         SpringApplication.run(Application.class, args);


     }


}

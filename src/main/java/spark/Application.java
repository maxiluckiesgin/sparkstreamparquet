package spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

//@ComponentScan({"com.maxiluckies.apache"})
@SpringBootApplication
public class Application{


     public static void main(String[] args) throws InterruptedException {
         spark.KafkaSpark kafkaSpark = new spark.KafkaSpark();

         SpringApplication.run(Application.class, args);

         kafkaSpark.startStream();


     }


}

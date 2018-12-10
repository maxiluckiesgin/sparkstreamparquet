
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;


@SpringBootApplication
public class Application{


     public static void main(String[] args) throws InterruptedException {
         KafkaSpark kafkaSpark = new KafkaSpark();

         kafkaSpark.startStream();


     }


}

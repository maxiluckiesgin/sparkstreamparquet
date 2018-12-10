package spark;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/message")
public class RequestHandler {

    @GetMapping("/")
    public String getBooks() {
        return "Get all books";
    }

    @GetMapping("/{id}")
    public String getBooksById(@PathVariable String id){
        return "Get book by id = " + id;
    }

    @PostMapping("/")
    public String postBooks(HttpEntity<String> httpEntity) {
        KafkaProducer kafkaProducer = new KafkaProducer();
        kafkaProducer.kafkaProducer().send(kafkaProducer.producerRecord("sparkStream", httpEntity.getBody()));
        return httpEntity.getBody();
    }

    @PutMapping("/{id}")
    public String editBooksById(@PathVariable String id){
        return "Edit book by id = " + id;
    }

    @DeleteMapping("/{id}")
    public String deleteBooksById(@PathVariable String id){
        return "Delete book by id = " + id;
    }

}
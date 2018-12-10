package spark;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/book")
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
    public String postBooks() {
        return "Post book";
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
package kafka.training.person.producer;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import service.PersonJobGenerator;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
class ApplicationTests {

	PersonJobGenerator personJobGenerator = new PersonJobGenerator();

	@Test
	void testRead() throws IOException {

//		while (true) {
//			System.err.println(PersonJobGenerator.generate());
//		}


	}


}

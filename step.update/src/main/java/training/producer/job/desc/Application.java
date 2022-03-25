package training.producer.job.desc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import training.producer.job.desc.dao.JobDescRepository;
import training.producer.job.desc.entity.JobDesc;

import javax.annotation.PostConstruct;
import javax.transaction.Transactional;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Autowired
	JobDescRepository jobDescRepository;

	// spaghetti
	@PostConstruct
	public void addExistingJobsToBd() throws IOException {
		System.err.println("On post construct");
		InputStream is = getClass().getResourceAsStream("/data/jobs.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));

		String myLine = null;

		try  {
			while ( (myLine = reader.readLine()) != null ) {
				JobDesc jobDesc = new JobDesc(myLine);
				jobDescRepository.save(jobDesc);
				System.err.println(jobDesc);
			}
		} catch (Exception e) {
			System.err.println("Exception: " + e.getMessage());
			e.printStackTrace();
		}

	}

}
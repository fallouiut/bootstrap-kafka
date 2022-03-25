package jobstat.consumer.kafka.training.manager;

import com.mycorp.mynamespace.JobStat;
import jobstat.consumer.kafka.training.entity.JobStatPersistent;
import jobstat.consumer.kafka.training.repository.JobStatRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.util.List;

@Transactional
@Service
public class JobStatManager {

    @Autowired
    JobStatRepository jobStatRepository;

    public List<JobStatPersistent> getAll() {
        return jobStatRepository.findAll();
    }

    public void insertOrUpdate(JobStat jobStat) {

        JobStatPersistent jobStatPersistent = jobStatRepository.findByName(jobStat.getJobName().toString()).orElse(null);

        if(jobStatPersistent == null) {// if not exists, we create it
            jobStatPersistent = new JobStatPersistent();
            jobStatPersistent.setName(jobStat.getJobName().toString());
            jobStatPersistent.setCount(jobStat.getCount());
            jobStatPersistent.setDescription( jobStat.getDescription() == null ? "": jobStat.getDescription().toString() );
            jobStatRepository.save(jobStatPersistent);
            System.err.println("\u001B[35mAdded: " + jobStatPersistent + "\u001B[0m" );
        } else {
            jobStatPersistent.setCount(jobStat.getCount());
            jobStatPersistent.setDescription( jobStat.getDescription() == null ? "": jobStat.getDescription().toString() );
            System.err.println("\u001B[36mUpdated: " + jobStatPersistent + "\u001B[0m" );
        }

    }

}

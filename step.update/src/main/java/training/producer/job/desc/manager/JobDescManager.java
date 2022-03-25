package training.producer.job.desc.manager;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import training.producer.job.desc.dao.JobDescRepository;
import training.producer.job.desc.entity.JobDesc;

import java.util.ArrayList;
import java.util.List;

@Transactional
@Service
public class JobDescManager {

    @Autowired
    JobDescRepository jobDescRepository;

    public void addJobDescription(JobDesc object) throws Exception {
        JobDesc jobDesc = jobDescRepository.findById(object.getId())
            .orElseThrow( () -> new Exception("Required job doesn't exists") );

        jobDesc.setDescription(object.getDescription());
        jobDescRepository.save(jobDesc);
    }

    public List<JobDesc> getAllJobDescs() {
        List<JobDesc> jobDescs = new ArrayList<>();
        jobDescs.addAll(jobDescRepository.findAll());
        return jobDescs;
    }

}

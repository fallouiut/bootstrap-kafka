package jobstat.consumer.kafka.training.controller;


import jobstat.consumer.kafka.training.entity.JobStatPersistent;
import jobstat.consumer.kafka.training.manager.JobStatManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/app")
public class JobStatController {

    @Autowired
    JobStatManager jobStatManager;

    // ajoute une description à un job
    @GetMapping("/get-stats")
    public ResponseEntity<Map<String, Object>> addJobDesc() {
        Map<String, Object> result = new HashMap<>();

        List<JobStatPersistent> jobStats = jobStatManager.getAll();

        Collections.sort(jobStats, new Comparator<JobStatPersistent>() {
            @Override
            public int compare(JobStatPersistent js1, JobStatPersistent js2) {
                return js2.getCount()-js1.getCount();
            }
        });

        result.put("jobStats", jobStats);
        return ResponseEntity.ok(result);
    }

}

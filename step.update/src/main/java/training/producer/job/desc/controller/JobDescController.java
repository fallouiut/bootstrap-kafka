package training.producer.job.desc.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;
import training.producer.job.desc.entity.JobDesc;
import training.producer.job.desc.manager.JobDescManager;

import javax.validation.Valid;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/app")
public class JobDescController {

    @Autowired
    JobDescManager jobDescManager;

    // ajoute une description à un job
    @PostMapping("/add-desc")
    public ResponseEntity<Map<String, Object>> addJobDesc(@RequestBody @Valid JobDesc jobDesc, BindingResult bindingResult) {
        Map<String, Object> result = new HashMap<>();

        // can be reduced by @ControllerAdvice
        try {

            if(bindingResult.hasErrors()) {
                throw new Exception ("Invalid data sent");
            }

            jobDescManager.addJobDescription(jobDesc);

        } catch (Exception e) {
            result.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(result);
        }

        return ResponseEntity.ok(result);
    }

    // ajoute une description à la
    @GetMapping("/get-jobs")
    public ResponseEntity<Map<String, Object>> getAllJobs() {
        Map<String, Object> result = new HashMap<>();
        System.err.println("One job descs requests");

        // can be reduced by @ControllerAdvice
        try {

            List<JobDesc> jobDescs = jobDescManager.getAllJobDescs();

            if(jobDescs == null || jobDescs.size() <= 0)  {
                throw new Exception("No data, try to re-launch app");
            }

            result.put("jobDescs", jobDescs);
            return ResponseEntity.ok(result);

        } catch (Exception e) {
            result.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(result);
        }
    }


}

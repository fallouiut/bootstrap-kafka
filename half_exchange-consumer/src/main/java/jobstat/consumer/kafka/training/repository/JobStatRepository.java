package jobstat.consumer.kafka.training.repository;

import jobstat.consumer.kafka.training.entity.JobStatPersistent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface JobStatRepository extends JpaRepository<JobStatPersistent, Integer> {

    Optional<JobStatPersistent> findByName(String name);

}

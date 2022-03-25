package training.producer.job.desc.dao;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import training.producer.job.desc.entity.JobDesc;


@Repository
@Transactional
public interface JobDescRepository extends JpaRepository<JobDesc, Integer> {}

package training.producer.job.desc.entity;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Objects;

@Entity(name = "JobDesc")
@Table(name = "T_JobDesc")
public class JobDesc implements Serializable {

    @Id()
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int id;

    @Column(name="jd_name", length = 100, nullable = false)
    private String jobName;

    @Column(name="jd_desc", length = 300, nullable = true)
    private String description;

    public JobDesc() {

    }

    public JobDesc(String jobName) {
        this.jobName = jobName;
        this.description = null;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobDesc jobDesc = (JobDesc) o;
        return id == jobDesc.id && Objects.equals(jobName, jobDesc.jobName) && Objects.equals(description, jobDesc.description);
    }

    @Override
    public String toString() {
        return "JobDesc{" +
                "id=" + id +
                ", jobName='" + jobName + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}

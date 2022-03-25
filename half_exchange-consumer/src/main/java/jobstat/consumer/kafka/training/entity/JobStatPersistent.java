package jobstat.consumer.kafka.training.entity;

import javax.persistence.*;
import java.io.Serializable;

@Entity(name= "JobStat")
@Table(name= "JobStat")
public class JobStatPersistent implements Serializable{

    @Id()
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int id;

    @Column(name="js_name", length = 100, nullable = false)
    private String name;

    @Column(name="js_desc", length = 300, nullable = true)
    private String description;

    @Column(name="js_count", length = 300, nullable = true)
    private int count;

    public JobStatPersistent() {

    }

    public JobStatPersistent(String name, String desc, int count) {
        this.name = name;
        this.description = desc;
        this.count = count;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

//
//    @Override
//    public int compareTo(Object o) {
//        JobStatPersistent jobStatPersistent = (JobStatPersistent) o;
//
//        return count-jobStatPersistent.count;
//    }

}

package service;

import com.mycorp.mynamespace.PersonJob;

import java.io.*;
import java.util.Random;

public class PersonJobGenerator {

    public PersonJob generate() {
        try {
            String name = generateName(); //generer
            String job  = generateJob(); //generer

            PersonJob person = PersonJob.newBuilder()
                    .setPersonName(name)
                    .setJobName(job)
                    .build();

            return person;

        }catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }

        return null;

    }

    public String generateName() throws IOException {
        int line = new Random().nextInt(1000); // 1000 names in file
        return read("/data/names.txt", line);
    }

    public String generateJob() throws IOException {
        int line = new Random().nextInt(100); // 100 jobs in file
        return read("/data/jobs.txt", line);
    }

    private String read(String file, int line) throws IOException {
        String myLine= "";

        InputStream is = getClass().getResourceAsStream(file);

        BufferedReader reader = new BufferedReader(new InputStreamReader(is));

        for (int i = 0; ; ++i) {
            myLine = reader.readLine();
            if (i == line) break;
        }

        return myLine;
    }


}

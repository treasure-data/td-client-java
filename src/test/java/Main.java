import java.io.IOException;
import java.util.Properties;

import org.msgpack.unpacker.Unpacker;
import org.msgpack.unpacker.UnpackerIterator;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.Database;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobResult;
import com.treasure_data.model.JobSummary;

public class Main {
    static {
        try {
            Properties props = System.getProperties();
            props.load(Main.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
        } catch (IOException e) {
            // do something
        }   
    }

    public void doApp() throws ClientException {
        TreasureDataClient client = new TreasureDataClient();

        Job job = new Job(new Database("testdb"), "SELECT COUNT(1) FROM www_access");
        client.submitJob(job);
        String jobID = job.getJobID();
        System.out.println(jobID);

        while (true) {
            JobSummary.Status stat = client.showJobStatus(job);
            if (stat == JobSummary.Status.SUCCESS) {
                break;
            } else if (stat == JobSummary.Status.ERROR) {
                String msg = String.format("Job '%s' failed: got Job status 'error'", jobID);
                JobSummary js = client.showJob(job);
                if (js.getDebug() != null) {
                    System.out.println("cmdout:");
                    System.out.println(js.getDebug().getCmdout());
                    System.out.println("stderr:");
                    System.out.println(js.getDebug().getStderr());
                }
                throw new ClientException(msg);
            } else if (stat == JobSummary.Status.KILLED) {
                String msg = String.format("Job '%s' failed: got Job status 'killed'", jobID);
                throw new ClientException(msg);
            }

            try {
                Thread.sleep(2 * 1000);
            } catch (InterruptedException e) {
                // do something
            }
        }

        JobResult jobResult = client.getJobResult(job);
        Unpacker unpacker = jobResult.getResult();
        UnpackerIterator iter = unpacker.iterator();
        while (iter.hasNext()) {
            System.out.println(iter.next());
        }
    }

    public static void main(String[] args) throws Exception {
        new Main().doApp();
    }
}

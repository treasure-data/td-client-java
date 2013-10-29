import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPOutputStream;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.UnpackerIterator;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.ClientException;
import com.treasure_data.client.Config;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.Database;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobResult;
import com.treasure_data.model.Table;


public class MultiThreadMain {

    private ExecutorService executor;
    private int threadNum;

    public MultiThreadMain(int threadNum) {
        if (threadNum >= 10) {
            throw new IllegalArgumentException();
        }
        this.threadNum = threadNum;
        this.executor = Executors.newFixedThreadPool(threadNum);
    }

    void sendData(final TreasureDataClient client) throws Exception {
        final List<Table> tables = new ArrayList<Table>(threadNum);
        final List<byte[]> data = new ArrayList<byte[]>(threadNum);
        

        for (int i = 0; i < threadNum; i++) {
            String dbname = "mugadb";
            String tablenamepre = "foo";
            String tablename = tablenamepre + i;
            tables.add(new Table(new Database(dbname), tablename));
            data.add(createData());
        }

        // import data with multi-threading
        List<Future> futures = new ArrayList<Future>(threadNum);
        for (int i = 0; i < threadNum; i++) {
            final Table t = tables.get(i);
            final byte[] d = data.get(i);
            futures.add(executor.submit(new Runnable() {
                @Override public void run() {
                    try {
                        client.importData(t, d);
                        System.out.println("imported data by thread: " + toString());
                    } catch (ClientException e) {
                        e.printStackTrace();
                    }
                }
            }));
        }

        // wait
        for (Future f : futures) {
            f.get();
        }

        executor.shutdownNow();
    }

    void sendData2(final Properties props) throws Exception {
        final List<Table> tables = new ArrayList<Table>(threadNum);
        final List<byte[]> data = new ArrayList<byte[]>(threadNum);
        final List<TreasureDataClient> clients = new ArrayList<TreasureDataClient>(threadNum);

        for (int i = 0; i < threadNum; i++) {
            String dbname = "mugadb";
            String tablenamepre = "foo";
            String tablename = tablenamepre + i;
            tables.add(new Table(new Database(dbname), tablename));
            data.add(createData());

            Config conf = new Config(props);
            conf.setCredentials(new TreasureDataCredentials(props));
            clients.add(new TreasureDataClient(props));
        }

        // import data with multi-threading
        List<Future> futures = new ArrayList<Future>(threadNum);
        for (int i = 0; i < threadNum; i++) {
            final Table t = tables.get(i);
            final byte[] d = data.get(i);
            final TreasureDataClient c = clients.get(i);
            futures.add(executor.submit(new Runnable() {
                @Override public void run() {
                    try {
                        c.importData(t, d);
                        System.out.println("imported data by thread: " + toString());
                    } catch (ClientException e) {
                        e.printStackTrace();
                    }
                }
            }));
        }

        // wait
        for (Future f : futures) {
            f.get();
        }

        executor.shutdownNow();
    }

    byte[] createData() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzout = new GZIPOutputStream(out);
        MessagePack msgpack = new MessagePack();
        Packer packer = msgpack.createPacker(gzout);

        for (int i = 0; i < 1000; i++) {
            long time = System.currentTimeMillis() / 1000;
            Map<String, Object> record = new HashMap<String, Object>();
            record.put("name", "muga:" + i);
            record.put("id", i);
            record.put("time", time);
            packer.write(record);
        }

        gzout.finish();
        return out.toByteArray();
    }

    public static void main(String[] args) throws Exception {
        Properties props = System.getProperties();
        //props.setProperty("td.api.key", "9b5e449696e769555a9a0f8534960ce0acb94c09");
        props.setProperty("td.api.key", "5b7158fd4ad7ee57bd3c0e49607ec72430a99c39");
        props.setProperty("td.api.server.host", "api.treasure-data.com");
        props.setProperty("td.api.server.port", "80");

        Config conf = new Config(props);
        conf.setCredentials(new TreasureDataCredentials(props));
        TreasureDataClient client = new TreasureDataClient(props);

        MultiThreadMain main = new MultiThreadMain(8);
        main.sendData(client);
        //main.sendData2(props);
//        JobResult jr = client.getJobResult(new Job("951687"));
//        UnpackerIterator iter = jr.getResult().iterator();
//        long c = 0L;
//        while (iter.hasNext()) {
//            System.out.println(iter.next());
//            c++;
//        }
//        System.out.println(c);
    }
}

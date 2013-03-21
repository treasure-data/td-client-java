package com.treasure_data.client;

import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.junit.Ignore;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;
import org.msgpack.unpacker.UnpackerIterator;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.model.GetJobResultRequest;
import com.treasure_data.model.GetJobResultResult;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobResult;
import com.treasure_data.model.JobResult2;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.ShowJobRequest;

public class TestGetJobResult {
    // TODO #MN need test more

    @Test @Ignore
    public void testGetJobResult00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        GetJobResultRequest request = new GetJobResultRequest(
                new JobResult(new Job("1515829")));
        GetJobResultResult result = clientAdaptor.getJobResult(request);
        System.out.println(result.getJob().getJobID());
        Unpacker unpacker = result.getJobResult().getResult();
        UnpackerIterator iter = unpacker.iterator();
        int cnt = 0;
        while (iter.hasNext()) {
            cnt++;
            System.out.println(iter.next());
        }
        unpacker.close();
        System.out.println(cnt);
    }

    @Test @Ignore
    public void testGetJobResult02() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        JobResult2 jr = (JobResult2) clientAdaptor.getJobResult(
                new GetJobResultRequest(new JobResult2(new Job("1515829")))).getJobResult();
        System.out.println(jr.getJob().getJobID());

        GZIPInputStream gzin = new GZIPInputStream(jr.getResultInputStream());
        Unpacker unpacker = new MessagePack().createUnpacker(gzin);
        UnpackerIterator iter = unpacker.iterator();
        int cnt = 0;
        while (iter.hasNext()) {
            cnt++;
            System.out.println(iter.next());
        }
        unpacker.close();

        jr.close();
        System.out.println("fin");
    }
}

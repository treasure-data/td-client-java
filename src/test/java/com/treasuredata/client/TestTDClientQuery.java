package com.treasuredata.client;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
/**
 * Created by ttruong on 2018-11-12.
 */
public class TestTDClientQuery
{
    private static final String SAMPLE_DB = "automation_data";
    private static final String SAMPLE_TB = "automation_result";
    public static final Logger logger = LoggerFactory.getLogger(TestTDClientQuery.class);
    private List<String> savedQueries = new ArrayList<>();
    private TDClient client;

    @Before
    public void setUp() throws Exception {
        client = TDClient.newClient();
    }

    @After
    public void tearDown()
            throws Exception
    {
        for (String name : savedQueries) {
            try {
                client.deleteSavedQuery(name);
            }
            catch (Exception e) {
                logger.error("Failed to delete query: {}", name, e);
            }
        }
        client.close();
    }

    @Test
    public void sellectSome(){
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("TestTDClientQuery -c %s -l %s -e %s -db %s -tb %s -m %s -M %s", "items,passed,failed", "10", "presto", SAMPLE_DB, SAMPLE_TB, "1510480920", "1541930460");
        assertTrue(executeQuery(query, stringToArray(cmd)));
    }

    @Test
    public void sellectAll(){
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("TestTDClientQuery -l %s -e %s -db %s -tb %s -m %s -M %s", "10", "presto", SAMPLE_DB, SAMPLE_TB, "1510480920", "1541930460");
        assertTrue(executeQuery(query, stringToArray(cmd)));
    }

    @Test
    public void noTime(){
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("TestTDClientQuery -l %s -e %s -db %s -tb %s", "10", "presto", SAMPLE_DB, SAMPLE_TB);
        assertTrue(executeQuery(query, stringToArray(cmd)));
    }

    @Test
    public void onlyMin(){
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("TestTDClientQuery -l %s -e %s -db %s -tb %s -m %s", "10", "presto", SAMPLE_DB, SAMPLE_TB, "1510480920");
        assertTrue(executeQuery(query, stringToArray(cmd)));
    }

    @Test
    public void onlyMax(){
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("TestTDClientQuery -l %s -e %s -db %s -tb %s -M %s", "10", "presto", SAMPLE_DB, SAMPLE_TB, "1510480920");
        assertTrue(executeQuery(query, stringToArray(cmd)));
    }

    @Test
    public void maxLessThanMin(){
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("TestTDClientQuery -l %s -e %s -db %s -tb %s -m %s -M %s", "10", "presto", SAMPLE_DB, SAMPLE_TB, "1541930460", "1510480920");
        assertFalse(executeQuery(query, stringToArray(cmd)));
    }

    @Test
    public void wrongTable(){
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("TestTDClientQuery -l %s -e %s -db %s -tb %s -m %s", "10", "presto", SAMPLE_DB, "wrong_table", "1541930460");
        assertFalse(executeQuery(query, stringToArray(cmd)));
    }

    @Test
    public void wrongDatabase(){
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("TestTDClientQuery -l %s -e %s -db %s -tb %s -m %s", "10", "presto", "wrong_db", SAMPLE_TB, "1541930460");
        assertFalse(executeQuery(query, stringToArray(cmd)));
    }

    public String [] stringToArray(String string){
        return string.split(" ");
    }

    public boolean executeQuery (TDClientQuery query, String [] cmd){
        try{
            query.update(parseCommand(cmd));
            String jobId = query.executeQuery(client,query.createQuery());
            // Wait until the query finishes
            ExponentialBackOff backOff = new ExponentialBackOff();
            TDJobSummary job = client.jobStatus(jobId);
            while (!job.getStatus().isFinished()) {
                Thread.sleep(backOff.nextWaitTimeMillis());
                job = client.jobStatus(jobId);
            }

            // Read the detailed job information
            TDJob jobInfo = client.jobInfo(jobId);
            System.out.println("log:\n" + jobInfo.getCmdOut());
            System.out.println("error log:\n" + jobInfo.getStdErr());
            // Read the job results in msgpack.gz format
            client.jobResult(jobId, TDResultFormat.MESSAGE_PACK_GZ, new Function<InputStream, Integer>()
            {
                @Override
                public Integer apply(InputStream input)
                {
                    int count = 0;
                    try {
                        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(input));
                        while (unpacker.hasNext()) {
                            // Each row of the query result is array type value (e.g., [1, "name", ...])
                            ArrayValue array = unpacker.unpackValue().asArrayValue();
                            System.out.println(array);
                            count++;
                        }
                        unpacker.close();
                    }
                    catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                    return count;
                }
            });
        } catch(Exception e){
            logger.error("Exception: " + e.toString());
            e.printStackTrace();
            return false;
        }
        return true;
    }

    // Parse parameters provided from command line
    public HashMap parseCommand(String [] args){
        HashMap result = new HashMap();

        // Args[0] should be command name so options and values can be retrieved starting from args[1]
        for (int i = 1; i < args.length; i += 2){

            // Options should start wit '-'
            if (!args[i].startsWith("-")){
                logger.error("Option " + args[i] + " should start with '-'");
                break;
            }

            // No option or value provided
            if (args[i].length() < 2){
                logger.error("Option " + args[i] + " should has lenght greater than 2");
                break;
            }

            // parameters into a java hash map
            result.put(args[i], args[i + 1]);
        }
        return result;
    }
}

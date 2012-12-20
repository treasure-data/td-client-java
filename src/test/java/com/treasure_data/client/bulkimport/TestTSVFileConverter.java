package com.treasure_data.client.bulkimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;

public class TestTSVFileConverter {
    @Test
    public void testConvertDataViaStream() throws Exception {
        {
            String columnList = "c0,c1,time";
            long baseTime = System.currentTimeMillis() / 1000;

            FileConverter conv = new TSVFileConverter();
            List<String> columns = conv.initColumns(columnList);
            byte[] bytes = (
                    "00\t01\t" + baseTime + "\n" +
                    "10\t11\t" + baseTime + "\n" +
                    "20\t21\t" + baseTime + "\n").getBytes();
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            conv.convertStream(in, out, columns);

            byte[] dst = out.toByteArray();
            GZIPInputStream dstStream = new GZIPInputStream(new ByteArrayInputStream(dst));
            Unpacker dstUnpacker = new MessagePack().createUnpacker(dstStream);
            for (int i = 0; i < 3; i++) {
                Map<String, Object> dstMap = FileConverter.tmpl.read(dstUnpacker, null);
                for (int j = 0; j < columns.size() - 1; j++) {
                    assertEquals("" + i + j, dstMap.get("c" + j));
                }
                assertEquals(baseTime, dstMap.get("time"));
            }
        }
    }

    @Test
    public void testConvertInvalidDataViaStream() throws Exception {
        {
            String columnList = "c0,time";
            long baseTime = System.currentTimeMillis() / 1000;

            FileConverter conv = new TSVFileConverter();
            List<String> columns = conv.initColumns(columnList);
            byte[] bytes = (
                    "00\t01\t" + baseTime + "\n" +
                    "10\t11\t" + baseTime + "\n" +
                    "20\t21\t" + baseTime + "\n").getBytes();
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                conv.convertStream(in, out, columns);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ClientException);
            }
        }
    }
}

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

public class TestFileConverter {

    public static class MockFileConverter extends FileConverter {
        @Override
        public String getDelimiter() {
            return "/";
        }
    }

    @Test
    public void testValidateColumnList() throws Exception {
        { // run successfully
            String columnList = "c0,c1,c2,time";
            FileConverter conv = new MockFileConverter();
            List<String> cs = conv.initColumns(columnList);
            assertEquals("c0", cs.get(0));
            assertEquals("c1", cs.get(1));
            assertEquals("c2", cs.get(2));
            assertEquals("time", cs.get(3));
        }
        { // run successfully
            String columnList = "time,c0,c1,c2";
            FileConverter conv = new MockFileConverter();
            List<String> cs = conv.initColumns(columnList);
            assertEquals("time", cs.get(0));
            assertEquals("c0", cs.get(1));
            assertEquals("c1", cs.get(2));
            assertEquals("c2", cs.get(3));
        }
        { // 'time' is not included in colum list
            String columnList = "c0, c1, c2";
            FileConverter conv = new MockFileConverter();
            try {
                conv.initColumns(columnList);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ClientException);
            }
        }
    }

    @Test
    public void testConvertDataViaStream() throws Exception {
        {
            String columnList = "c0,c1,time";
            long baseTime = System.currentTimeMillis() / 1000;

            FileConverter conv = new MockFileConverter();
            List<String> columns = conv.initColumns(columnList);
            byte[] bytes = (
                    "00/01/" + baseTime + "\n" +
                    "10/11/" + baseTime + "\n" +
                    "20/21/" + baseTime + "\n").getBytes();
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

            FileConverter conv = new MockFileConverter();
            List<String> columns = conv.initColumns(columnList);
            byte[] bytes = (
                    "00/01/" + baseTime + "\n" +
                    "10/11/" + baseTime + "\n" +
                    "20/21/" + baseTime + "\n").getBytes();
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

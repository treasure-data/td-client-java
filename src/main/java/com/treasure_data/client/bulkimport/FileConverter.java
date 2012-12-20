//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2012 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.treasure_data.client.bulkimport;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.zip.GZIPOutputStream;

import org.msgpack.MessagePack;
import org.msgpack.MessageTypeException;
import org.msgpack.packer.Packer;
import org.msgpack.template.Template;
import org.msgpack.template.Templates;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;

public abstract class FileConverter {
    static final Template<Map<String, Object>> tmpl = new ExtendedTemplate(); // TODO should change name

    static class ExtendedTemplate implements Template<Map<String, Object>> {
        @Override
        public void write(Packer pk, Map<String, Object> v) throws IOException {
            write(pk, v, false);
        }

        @Override
        public void write(Packer pk, Map<String, Object> v, boolean required)
                throws IOException {
            if (!(v instanceof Map)) {
                if (v == null) {
                    if (required) {
                        throw new MessageTypeException("Attempted to write null");
                    }
                    pk.writeNil();
                    return;
                }
                throw new MessageTypeException("Target is not a Map but " + v.getClass());
            }
            pk.writeMapBegin(v.size());
            for (Map.Entry<String, Object> pair : v.entrySet()) {
                String key = pair.getKey();
                Templates.TString.write(pk, pair.getKey());
                if (!key.equals("time")) {
                    Templates.TString.write(pk, (String) pair.getValue());
                } else {
                    Templates.TLong.write(pk, (Long) pair.getValue());
                }
            }
            pk.writeMapEnd();
        }

        @Override
        public Map<String, Object> read(Unpacker u, Map<String, Object> to)
                throws IOException {
            return read(u, to, false);
        }

        @Override
        public Map<String, Object> read(Unpacker u, Map<String, Object> to,
                boolean required) throws IOException {
            if (!required && u.trySkipNil()) {
                return null;
            }
            int n = u.readMapBegin();
            Map<String, Object> map;
            if (to != null) {
                map = (Map<String, Object>) to;
                map.clear();
            } else {
                map = new HashMap<String, Object>(n);
            }
            for (int i = 0; i < n; i++) {
                String key = Templates.TString.read(u, null);
                Object value;
                if (!key.equals("time")) {
                    value = Templates.TString.read(u, null);
                } else {
                    value = Templates.TLong.read(u, null);
                }
                map.put(key, value);
            }
            u.readMapEnd();
            return map;
        }
    }

    private GZIPOutputStream gzout;

    private int timeColIndex;

    public void convertFile(String inputFileName, String outputFileName, String columnList)
            throws ClientException {
        // validation and initialization
        File inputFile = initInput(inputFileName);
        File outputFile = initOutput(outputFileName);
        List<String> columns = initColumns(columnList);

        // create file in/out stream
        InputStream in = null;
        OutputStream out = null;
        try {
            in = new FileInputStream(inputFile);
        } catch (FileNotFoundException e) {
            throw new ClientException(e);
        }
        try {
            out = new FileOutputStream(outputFile);
        } catch (FileNotFoundException e) {
            throw new ClientException(e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    throw new ClientException(e);
                }
            }
        }

        // convert
        convertStream(in, out, columns);
    }

    public void convertStream(InputStream in, OutputStream out, List<String> columns)
            throws ClientException {

        // create reader/packer
        Packer packer = createGZMsgpackPacker(out);
        BufferedReader reader = createBufferedReader(in);

        try {
            String line = null;
            while ((line = reader.readLine()) != null) {
                // convert
                convertString(line, packer, columns);
            }
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            // close
            closeGZMsgpackPacker(packer);
            closeBufferedReader(reader);
        }
    }

    public void convertString(String line, Packer packer, List<String> columns)
            throws ClientException {
        List<String> values = new ArrayList<String>();
        StringTokenizer t = new StringTokenizer(line, getDelimiter());
        while (t.hasMoreTokens()) {
            values.add(t.nextToken());
        }

        if (values.size() != columns.size()) {
            throw new ClientException("" +
            		"Mismatch column number: # of record's column is " + values.size() +
            		", # of specified colum names is " + columns.size());
        }

        Map<String, Object> record = new HashMap<String, Object>();
        for (int i = 0; i < values.size(); i++) {
            if (timeColIndex == i) {
                record.put(columns.get(i), Long.parseLong(values.get(i)));
            } else {
                record.put(columns.get(i), values.get(i));
            }
        }

        try {
           tmpl.write(packer, record);
        } catch (IOException e) {
            throw new ClientException(e);
        }
    }

    protected abstract String getDelimiter();

    File initInput(String inputFileName) throws ClientException {
        File inputFile = new File(inputFileName);
        if (!inputFile.isFile()) {
            throw new ClientException("Not found input file: " + inputFile.getName());
        }
        return inputFile;
    }

    File initOutput(String outputFileName) throws ClientException {
        File outputFile = new File(outputFileName);
        if (!outputFile.exists()) {
            try {
                outputFile.createNewFile();
            } catch (IOException e) {
                throw new ClientException(e);
            }
        }
        return outputFile;
    }

    List<String> initColumns(String columnList) throws ClientException {
        StringTokenizer t = new StringTokenizer(columnList, " ,");
        List<String> ret = new ArrayList<String>();
        while (t.hasMoreTokens()) {
            ret.add(t.nextToken());
        }

        timeColIndex = ret.indexOf("time");
        if (timeColIndex < 0) {
            throw new ClientException("'time' column is not included in column list: " + columnList);
        }

        return ret;
    }

    protected Packer createGZMsgpackPacker(OutputStream out) throws ClientException {
        try {
            MessagePack msgpack = new MessagePack();
            gzout = new GZIPOutputStream(new BufferedOutputStream(out));
            return msgpack.createPacker(gzout);
        } catch (IOException e) {
            throw new ClientException(e);
        }
    }

    protected void closeGZMsgpackPacker(Packer packer) throws ClientException {
        if (gzout != null) {
            try {
                gzout.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                gzout.finish();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (packer != null) {
            try {
                packer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    protected BufferedReader createBufferedReader(InputStream in) throws ClientException {
        return new BufferedReader(new InputStreamReader(in));
    }

    protected void closeBufferedReader(BufferedReader reader) throws ClientException {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                throw new ClientException(e);
            }
        }
    }
}

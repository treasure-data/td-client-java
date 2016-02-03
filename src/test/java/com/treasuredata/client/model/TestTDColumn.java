/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.treasuredata.client.model;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static com.treasuredata.client.model.TDColumnType.FLOAT;
import static com.treasuredata.client.model.TDColumnType.INT;
import static com.treasuredata.client.model.TDColumnType.LONG;
import static com.treasuredata.client.model.TDColumnType.STRING;
import static com.treasuredata.client.model.TDColumnType.newArrayType;
import static com.treasuredata.client.model.TDColumnType.newMapType;
import static com.treasuredata.client.model.TDColumnType.parseColumnType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public class TestTDColumn
{
    @Test
    public void column()
    {
        TDColumn t = new TDColumn("time", LONG, "time".getBytes(UTF_8));
        assertEquals("time", t.getName());
        assertEquals(LONG, t.getType());
        assertEquals("time", new String(t.getKey(), StandardCharsets.UTF_8));
        assertArrayEquals(new String[] {"time", "long", "time"}, t.getTuple());

        TDColumn t2 = new TDColumn("time", LONG);
        assertEquals(t, t2);
        assertEquals(t.hashCode(), t2.hashCode());
        assertFalse(t.equals(""));

        // hashCode, equals test
        Set<TDColumn> columnSet = new HashSet<>();
        columnSet.add(t);
        columnSet.add(t);
        assertTrue(columnSet.contains(t));
        columnSet.remove(t);
        assertFalse(columnSet.contains(t));
    }

    @Test
    public void parseRenamedColumns()
    {
        TDColumn t = TDColumn.parseTuple(new String[] {"mycol", "string", "mycol_prev"});
        assertEquals("mycol", t.getName());
        assertEquals(TDColumnType.STRING, t.getType());
        assertEquals("mycol_prev", new String(t.getKey(), StandardCharsets.UTF_8));
    }

    @Test
    public void parsePrimitiveColumnTypes()
    {
        // primitive type set
        Set<TDColumnType> primitives = ImmutableSet.copyOf(TDColumnType.primitiveTypes);

        // primitive types
        for (String name : new String[] {"int", "long", "float", "double", "string"}) {
            TDColumnType t = parseColumnType(name);
            assertFalse(t.isArrayType());
            assertFalse(t.isMapType());
            assertTrue(t.isPrimitive());
            assertTrue(TDColumnType.primitiveTypes.contains(t));
            assertTrue(primitives.contains(t));
            assertEquals(name, t.getTypeName().toString());
            try {
                t.getArrayElementType();
                fail("should not reach here");
            }
            catch (UnsupportedOperationException e) {
                // OK
            }
            try {
                t.getMapKeyType();
                fail("should not reach here");
            }
            catch (UnsupportedOperationException e) {
                // OK
            }
            try {
                t.getMapValueType();
                fail("should not reach here");
            }
            catch (UnsupportedOperationException e) {
                // OK
            }
        }
    }

    @Test
    public void parseArrayType()
    {
        // array type
        TDColumnType arrType = parseColumnType("array<int>");
        assertTrue(arrType.isArrayType());
        assertFalse(arrType.isMapType());
        assertFalse(arrType.isPrimitive());
        assertEquals(INT, arrType.getArrayElementType());
        try {
            arrType.getMapKeyType();
            fail("should not reach here");
        }
        catch (UnsupportedOperationException e) {
            // OK
        }
        try {
            arrType.getMapValueType();
            fail("should not reach here");
        }
        catch (UnsupportedOperationException e) {
            // OK
        }

        // nested array type
        TDColumnType a1 = newArrayType(newArrayType(FLOAT));
        String a1s = "array<array<float>>";
        assertEquals(a1s, a1.toString());
        TDColumnType a1p = parseColumnType(a1s);
        assertEquals(a1, a1p);
        assertEquals(a1, parseColumnType(" array< array <float> >"));

        TDColumnType a2 = newArrayType(newMapType(STRING, newArrayType(LONG)));
        String a2s = "array<map<string,array<long>>>";
        assertEquals(a2s, a2.toString());
        TDColumnType a2p = parseColumnType(a2s);
        assertEquals(a2, a2p);
    }

    @Test
    public void parseMapType()
    {
        TDColumnType m1 = newMapType(INT, STRING);
        assertEquals("map<int,string>", m1.toString());
        assertEquals(m1, parseColumnType(m1.toString()));
        assertEquals(m1, parseColumnType("map< int , string >"));

        TDColumnType m2 = newMapType(INT, newArrayType(newMapType(INT, FLOAT)));
        assertEquals("map<int,array<map<int,float>>>", m2.toString());
        assertEquals(m2, parseColumnType(m2.toString()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseInvalidType1()
    {
        TDColumnType.parseColumnType("int2");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseInvalidType2()
    {
        TDColumnType.parseColumnType("array[int]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseInvalidType3()
    {
        TDColumnType.parseColumnType("array<int]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseInvalidType4()
    {
        TDColumnType.parseColumnType("map<int>");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseInvalidType5()
    {
        TDColumnType.parseColumnType("map<int2>");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseInvalidType6()
    {
        TDColumnType.parseColumnType("map<int, int]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseInvalidType7()
    {
        TDColumnType.parseColumnType("map[int, int]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseUnknownType()
    {
        TDColumnType.parseColumnType("xint");
    }

    private static void checkSerialization(Object o)
            throws IOException
    {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(b);
        os.writeObject(o);
        os.close();
    }

    @Test
    public void serializableTest()
            throws Exception
    {
        checkSerialization(new TDColumn("int", TDColumnType.INT));
        checkSerialization(new TDColumn("str", TDColumnType.STRING));
        checkSerialization(new TDColumn("long", TDColumnType.LONG));
        checkSerialization(new TDColumn("double", TDColumnType.DOUBLE));
        checkSerialization(new TDColumn("float", TDColumnType.FLOAT));
        checkSerialization(newArrayType(TDColumnType.STRING));
        checkSerialization(newMapType(TDColumnType.INT, TDColumnType.STRING));
    }
}

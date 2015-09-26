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

import org.junit.Test;

import static com.treasuredata.client.model.TDColumnType.FLOAT;
import static com.treasuredata.client.model.TDColumnType.INT;
import static com.treasuredata.client.model.TDColumnType.LONG;
import static com.treasuredata.client.model.TDColumnType.STRING;
import static com.treasuredata.client.model.TDColumnType.newArrayType;
import static com.treasuredata.client.model.TDColumnType.newMapType;
import static com.treasuredata.client.model.TDColumnType.parseColumnType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public class TDColumnTest
{
    @Test
    public void parsePrimitiveColumnTypes()
    {
        // primitive types
        for (String name : new String[] {"int", "long", "float", "double", "string"}) {
            TDColumnType t = parseColumnType(name);
            assertFalse(t.isArrayType());
            assertFalse(t.isMapType());
            assertTrue(t.isPrimitive());
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

        TDColumnType m2 = newMapType(INT, newArrayType(newMapType(INT, FLOAT)));
        assertEquals("map<int,array<map<int,float>>>", m2.toString());
        assertEquals(m2, parseColumnType(m2.toString()));
    }
}

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
package com.treasuredata.client.api.model;

import com.google.common.base.Objects;

public class TDMapColumnType
        implements TDColumnType
{
    private TDColumnType keyType;
    private TDColumnType valueType;

    public TDMapColumnType(TDColumnType keyType, TDColumnType valueType)
    {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public TDColumnType getKeyType()
    {
        return keyType;
    }

    public TDColumnType getValueType()
    {
        return valueType;
    }

    @Override
    public String toString()
    {
        return "map<" + keyType + "," + valueType + ">";
    }

    @Override
    public boolean isPrimitive()
    {
        return false;
    }

    @Override
    public boolean isArrayType()
    {
        return false;
    }

    @Override
    public boolean isMapType()
    {
        return true;
    }

    @Override
    public TDPrimitiveColumnType asPrimitiveType()
    {
        return null;
    }

    @Override
    public TDArrayColumnType asArrayType()
    {
        return null;
    }

    @Override
    public TDMapColumnType asMapType()
    {
        return this;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TDMapColumnType other = (TDMapColumnType) obj;
        return Objects.equal(this.keyType, other.keyType) &&
                Objects.equal(this.valueType, other.valueType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(keyType, valueType);
    }
}

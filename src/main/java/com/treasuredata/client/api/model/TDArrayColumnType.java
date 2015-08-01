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

public class TDArrayColumnType
        implements TDColumnType
{
    private final TDColumnType elementType;

    public TDArrayColumnType(TDColumnType elementType)
    {
        this.elementType = elementType;
    }

    public TDColumnType getElementType()
    {
        return elementType;
    }

    @Override
    public String toString()
    {
        return "array<" + elementType + ">";
    }

    @Override
    public boolean isPrimitive()
    {
        return false;
    }

    @Override
    public boolean isArrayType()
    {
        return true;
    }

    @Override
    public boolean isMapType()
    {
        return false;
    }

    @Override
    public TDPrimitiveColumnType asPrimitiveType()
    {
        return null;
    }

    @Override
    public TDArrayColumnType asArrayType()
    {
        return this;
    }

    @Override
    public TDMapColumnType asMapType()
    {
        return null;
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
        TDArrayColumnType other = (TDArrayColumnType) obj;
        return Objects.equal(this.elementType, other.elementType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(elementType);
    }
}

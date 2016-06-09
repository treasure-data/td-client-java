package com.treasuredata.client.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

class StringToNumberSerializer
        extends StdSerializer<String>
{
    public StringToNumberSerializer()
    {
        super(String.class);
    }

    @Override
    public void serialize(String s, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException
    {
        jsonGenerator.writeNumber(Integer.valueOf(s));
    }
}

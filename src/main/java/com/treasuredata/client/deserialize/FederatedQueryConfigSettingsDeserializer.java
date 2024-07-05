package com.treasuredata.client.deserialize;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class FederatedQueryConfigSettingsDeserializer extends JsonDeserializer<String>
{
  @Override
  public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException
  {
    ObjectMapper mapper = (ObjectMapper) p.getCodec();
    ObjectNode root = mapper.readTree(p);
    return root.toString();
  }
}
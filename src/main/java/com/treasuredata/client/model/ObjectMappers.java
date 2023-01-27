package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;

/**
 * @deprecated Do not use this from application. This file made public for testing and is a subject to be deleted.
 */
@Deprecated
public class ObjectMappers
{
    private ObjectMappers()
    {
    }

    /**
     * Lazy instantiation wrapper.
     */
    private static class Lazy
    {
        private static final ObjectMapper COMPACT_MAPPER = createCompactMapper();
    }

    /**
     * Get an {@link ObjectMapper} that omits null and absent fields.
     */
    public static ObjectMapper compactMapper()
    {
        return Lazy.COMPACT_MAPPER;
    }

    private static ObjectMapper createCompactMapper()
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JsonOrgModule());
        mapper.registerModule(new Jdk8Module().configureAbsentsAsNulls(false));
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }
}

package org.apache.cstore.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class JsonUtil
{
    private JsonUtil()
    {
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static <T> T read(File file, Class<T> clazz)
    {
        try {
            return OBJECT_MAPPER.readValue(file, clazz);
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static <T> byte[] write(T object)
    {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(object);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}

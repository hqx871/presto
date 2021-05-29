package org.apache.cstore.dictionary;

public interface Dictionary<T>
{
    int encodeId(String value);

    T decodeValue(int id);

    int count();

    int maxEncodeId();
}

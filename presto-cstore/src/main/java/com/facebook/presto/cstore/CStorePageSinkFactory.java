package com.facebook.presto.cstore;

import com.facebook.presto.spi.ConnectorPageSink;

import java.util.OptionalInt;

public interface CStorePageSinkFactory
{
    ConnectorPageSink create(OptionalInt day, OptionalInt bucketNumber);
}

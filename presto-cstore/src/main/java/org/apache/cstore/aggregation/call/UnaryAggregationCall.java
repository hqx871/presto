package org.apache.cstore.aggregation.call;

import org.apache.cstore.aggregation.AggregationCall;

public abstract class UnaryAggregationCall
        implements AggregationCall
{
    protected final int inputChannel;

    protected UnaryAggregationCall(int inputChannel)
    {
        this.inputChannel = inputChannel;
    }
}

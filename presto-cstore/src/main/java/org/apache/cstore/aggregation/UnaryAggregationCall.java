package org.apache.cstore.aggregation;

abstract class UnaryAggregationCall
        implements AggregationCall
{
    protected final int inputChannel;

    protected UnaryAggregationCall(int inputChannel)
    {
        this.inputChannel = inputChannel;
    }
}

package github.cstore.aggregation.call;

import github.cstore.aggregation.AggregationCall;

public abstract class UnaryAggregationCall
        implements AggregationCall
{
    protected final int inputChannel;

    protected UnaryAggregationCall(int inputChannel)
    {
        this.inputChannel = inputChannel;
    }
}

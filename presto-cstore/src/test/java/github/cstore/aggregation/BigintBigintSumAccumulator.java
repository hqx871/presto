//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package github.cstore.aggregation;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.AggregationUtils;
import com.facebook.presto.operator.aggregation.LambdaProvider;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.WindowIndex;
import com.facebook.presto.sql.gen.CompilerOperations;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.aggregation.LongSumAggregation.combine;
import static java.util.Objects.requireNonNull;

public final class BigintBigintSumAccumulator
        implements Accumulator
{
    private final AccumulatorStateSerializer stateSerializer0;
    private final AccumulatorStateFactory stateFactory0;
    private final SingleNullableLongState state0;
    private final List<Integer> inputChannels;
    private final Optional<Integer> maskChannel;

    public void addIntermediate(Block block)
    {
        SingleNullableLongState scratchState0 = (SingleNullableLongState) this.stateFactory0.createSingleState();
        int rows = block.getPositionCount();

        for (int position = 0; CompilerOperations.lessThan(position, rows); ++position) {
            if (!block.isNull(position)) {
                SingleNullableLongState var10000 = this.state0;
                this.stateSerializer0.deserialize(block, position, (Object) scratchState0);
                combine(var10000, scratchState0);
            }
        }
    }

    public void evaluateIntermediate(BlockBuilder out)
    {
        this.stateSerializer0.serialize((Object) this.state0, out);
    }

    public BigintBigintSumAccumulator(List<AccumulatorStateDescriptor> stateDescriptors, List<Integer> inputChannels, Optional<Integer> maskChannel, List<LambdaProvider> lambdaProviders)
    {
        this.stateSerializer0 = ((AccumulatorStateDescriptor) stateDescriptors.get(0)).getSerializer();
        this.stateFactory0 = ((AccumulatorStateDescriptor) stateDescriptors.get(0)).getFactory();
        this.inputChannels = (List) requireNonNull((Object) inputChannels, "inputChannels is null");
        this.maskChannel = (Optional) requireNonNull((Object) maskChannel, "maskChannel is null");
        this.state0 = (SingleNullableLongState) this.stateFactory0.createSingleState();
    }

    public long getEstimatedSize()
    {
        long estimatedSize = 0L;
        estimatedSize += this.state0.getEstimatedSize();
        return estimatedSize;
    }

    public Type getFinalType()
    {
        return BigintType.BIGINT;
    }

    public Type getIntermediateType()
    {
        return BigintType.BIGINT;
    }

    public void addInput(Page page)
    {
        Block masksBlock = (Block) this.maskChannel.map(AggregationUtils.pageBlockGetter(page)).orElse(null);
        Block block0 = page.getBlock((Integer) this.inputChannels.get(0));
        int rows = page.getPositionCount();
        //int position = false;
        if (CompilerOperations.greaterThan(rows, 0) && (!(block0 instanceof RunLengthEncodedBlock) || !block0.isNull(0))) {
            for (int position = 0; CompilerOperations.lessThan(position, rows); ++position) {
                if (CompilerOperations.testMask(masksBlock, position) && !block0.isNull(position)) {
                    this.state0.setLong(this.state0.getLong() + BigintType.BIGINT.getLong(block0, position));
                }
            }
        }
    }

    public void addInput(WindowIndex index, List<Integer> channels, int startPosition, int endPosition)
    {
        for (int position = startPosition; position <= endPosition; ++position) {
            if (true && !index.isNull((Integer) channels.get(0), position)) {
                this.state0.setLong(this.state0.getLong() + index.getLong((Integer) channels.get(0), position));
            }
        }
    }

    public void evaluateFinal(BlockBuilder out)
    {
        out.writeLong(this.state0.getLong()).closeEntry();
    }
}

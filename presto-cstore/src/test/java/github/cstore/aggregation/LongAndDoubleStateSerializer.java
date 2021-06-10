//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package github.cstore.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.state.LongAndDoubleState;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.google.common.collect.ImmutableList;

public final class LongAndDoubleStateSerializer
        implements AccumulatorStateSerializer
{
    public void serialize(Object state, BlockBuilder out)
    {
        BlockBuilder rowBuilder = out.beginBlockEntry();
        double temp0 = ((LongAndDoubleState) state).getDouble();
        DoubleType.DOUBLE.writeDouble(rowBuilder, temp0);
        long temp2 = ((LongAndDoubleState) state).getLong();
        BigintType.BIGINT.writeLong(rowBuilder, temp2);
        out.closeEntry();
    }

    public void deserialize(Block block, int index, Object state)
    {
        Block row = block.getBlock(index);
        ((LongAndDoubleState) state).setDouble(DoubleType.DOUBLE.getDouble(row, 0));
        ((LongAndDoubleState) state).setLong(BigintType.BIGINT.getLong(row, 1));
    }

    public LongAndDoubleStateSerializer()
    {
    }

    public Type getSerializedType()
    {
        //todo this is mock data, use real code
        return RowType.anonymous(ImmutableList.of(BigintType.BIGINT, DoubleType.DOUBLE));
    }
}

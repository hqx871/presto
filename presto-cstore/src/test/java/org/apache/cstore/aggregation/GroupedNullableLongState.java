//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.cstore.aggregation;

import com.facebook.presto.array.BooleanBigArray;
import com.facebook.presto.array.LongBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import org.openjdk.jol.info.ClassLayout;

public final class GroupedNullableLongState
        extends AbstractGroupedAccumulatorState
        implements NullableLongState
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(GroupedNullableLongState.class).instanceSize();
    private LongBigArray longValues = new LongBigArray(0L);
    private BooleanBigArray nullValues = new BooleanBigArray(true);

    public GroupedNullableLongState()
    {
    }

    public long getLong()
    {
        return this.longValues.get(this.getGroupId());
    }

    public void setLong(long value)
    {
        this.longValues.set(this.getGroupId(), value);
    }

    public void ensureCapacity(long size)
    {
        this.longValues.ensureCapacity(size);
        this.nullValues.ensureCapacity(size);
    }

    public boolean isNull()
    {
        return this.nullValues.get(this.getGroupId());
    }

    public long getEstimatedSize()
    {
        long size = INSTANCE_SIZE;
        size += this.longValues.sizeOf();
        size += this.nullValues.sizeOf();
        return size;
    }

    public void setNull(boolean value)
    {
        this.nullValues.set(this.getGroupId(), value);
    }
}

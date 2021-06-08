//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.cstore.aggregation;

import com.facebook.presto.operator.aggregation.state.NullableLongState;
import org.openjdk.jol.info.ClassLayout;

public final class SingleNullableLongState
        implements NullableLongState
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(SingleNullableLongState.class).instanceSize();
    private long longValue;
    private boolean nullValue = true;

    public SingleNullableLongState()
    {
    }

    public long getLong()
    {
        return this.longValue;
    }

    public void setLong(long value)
    {
        this.longValue = value;
    }

    public boolean isNull()
    {
        return this.nullValue;
    }

    public long getEstimatedSize()
    {
        return INSTANCE_SIZE;
    }

    public void setNull(boolean value)
    {
        this.nullValue = value;
    }
}

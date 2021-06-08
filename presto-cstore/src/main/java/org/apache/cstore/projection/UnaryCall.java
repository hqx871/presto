package org.apache.cstore.projection;

import com.facebook.presto.common.type.Type;
import org.apache.cstore.column.VectorCursor;

import java.util.List;

public abstract class UnaryCall
        implements ScalarCall
{
    protected final Type type;
    protected final int inputChannel;
    protected final int outputChannel;

    protected UnaryCall(Type type, int inputChannel, int outputChannel)
    {
        this.type = type;
        this.inputChannel = inputChannel;
        this.outputChannel = outputChannel;
    }

    @Override
    public int getOutputChannel()
    {
        return outputChannel;
    }

    @Override
    public final void process(List<? extends VectorCursor> page, int size)
    {
        VectorCursor op0 = page.get(inputChannel);
        VectorCursor out = page.get(outputChannel);
        doProcess(op0, out, size);
    }

    protected abstract void doProcess(VectorCursor input, VectorCursor output, int size);
}

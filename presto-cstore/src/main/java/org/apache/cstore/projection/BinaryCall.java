package org.apache.cstore.projection;

import org.apache.cstore.column.VectorCursor;

import java.util.List;

public abstract class BinaryCall
        implements ScalarCall
{
    private final int inputChannel0;
    private final int inputChannel1;
    private final int outputChannel;

    protected BinaryCall(int inputChannel0, int inputChannel1, int outputChannel)
    {
        this.inputChannel0 = inputChannel0;
        this.inputChannel1 = inputChannel1;
        this.outputChannel = outputChannel;
    }

    @Override
    public final void process(List<? extends VectorCursor> page, int size)
    {
        VectorCursor op0 = page.get(inputChannel0);
        VectorCursor op1 = page.get(inputChannel1);
        VectorCursor out = page.get(outputChannel);
        doProcess(op0, op1, out, size);
    }

    @Override
    public void process(List<? extends VectorCursor> page, int[] positions, int size)
    {
        VectorCursor op0 = page.get(inputChannel0);
        VectorCursor op1 = page.get(inputChannel1);
        VectorCursor out = page.get(outputChannel);
        doProcess(op0, op1, out, positions, size);
    }

    @Override
    public int getOutputChannel()
    {
        return outputChannel;
    }

    protected abstract void doProcess(VectorCursor op0, VectorCursor op1, VectorCursor out, int[] positions, int size);

    protected abstract void doProcess(VectorCursor op0, VectorCursor op1, VectorCursor out, int size);
}

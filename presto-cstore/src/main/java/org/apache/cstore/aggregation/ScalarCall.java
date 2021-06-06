package org.apache.cstore.aggregation;

import org.apache.cstore.column.VectorCursor;

import java.util.List;

public interface ScalarCall
{
    void process(List<? extends VectorCursor> page, int size);
}

abstract class BinaryCall
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
        doPlus(op0, op1, out, size);
    }

    protected abstract void doPlus(VectorCursor op0, VectorCursor op1, VectorCursor out, int size);
}

class DoublePlusCall
        extends BinaryCall
{
    DoublePlusCall(int inputChannel0, int inputChannel1, int outputChannel)
    {
        super(inputChannel0, inputChannel1, outputChannel);
    }

    @Override
    protected void doPlus(VectorCursor op0, VectorCursor op1, VectorCursor out, int size)
    {
        for (int i = 0; i < size; i++) {
            out.writeDouble(i, op0.readDouble(i) + op1.readDouble(i));
        }
    }
}

class DoubleMinusCall
        extends BinaryCall
{
    DoubleMinusCall(int inputChannel0, int inputChannel1, int outputChannel)
    {
        super(inputChannel0, inputChannel1, outputChannel);
    }

    @Override
    protected void doPlus(VectorCursor op0, VectorCursor op1, VectorCursor out, int size)
    {
        for (int i = 0; i < size; i++) {
            out.writeDouble(i, op0.readDouble(i) - op1.readDouble(i));
        }
    }
}

class DoubleMultipleCall
        extends BinaryCall
{
    DoubleMultipleCall(int inputChannel0, int inputChannel1, int outputChannel)
    {
        super(inputChannel0, inputChannel1, outputChannel);
    }

    @Override
    protected void doPlus(VectorCursor op0, VectorCursor op1, VectorCursor out, int size)
    {
        for (int i = 0; i < size; i++) {
            out.writeDouble(i, op0.readDouble(i) * op1.readDouble(i));
        }
    }
}

class LongPlusCall
        extends BinaryCall
{
    LongPlusCall(int inputChannel0, int inputChannel1, int outputChannel)
    {
        super(inputChannel0, inputChannel1, outputChannel);
    }

    @Override
    protected void doPlus(VectorCursor op0, VectorCursor op1, VectorCursor out, int size)
    {
        for (int i = 0; i < size; i++) {
            out.writeLong(i, op0.readLong(i) + op1.readLong(i));
        }
    }
}
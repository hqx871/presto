package github.cstore.coder;

import java.nio.ByteBuffer;

public interface FixLenBufferCoder<T>
        extends BufferCoder<T>
{
    int size();

    FixLenBufferCoder<Integer> INTEGER = new FixLenBufferCoder<Integer>()
    {
        @Override
        public int size()
        {
            return Integer.BYTES;
        }

        @Override
        public Integer decode(ByteBuffer data)
        {
            return data.getInt(0);
        }

        @Override
        public ByteBuffer encode(Integer object)
        {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            return buffer.putInt(object);
        }
    };

    FixLenBufferCoder<Long> LONG = new FixLenBufferCoder<Long>()
    {
        @Override
        public int size()
        {
            return Long.BYTES;
        }

        @Override
        public Long decode(ByteBuffer data)
        {
            return data.getLong(0);
        }

        @Override
        public ByteBuffer encode(Long object)
        {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            return buffer.putLong(object);
        }
    };

    FixLenBufferCoder<Double> DOUBLE = new FixLenBufferCoder<Double>()
    {
        @Override
        public int size()
        {
            return Double.BYTES;
        }

        @Override
        public Double decode(ByteBuffer data)
        {
            return data.getDouble(0);
        }

        @Override
        public ByteBuffer encode(Double object)
        {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            return buffer.putDouble(object);
        }
    };

    class Raw
            implements FixLenBufferCoder<ByteBuffer>
    {
        private int size;

        public Raw(int size)
        {
            this.size = size;
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public ByteBuffer decode(ByteBuffer data)
        {
            return data;
        }

        @Override
        public ByteBuffer encode(ByteBuffer object)
        {
            return object;
        }
    }
}

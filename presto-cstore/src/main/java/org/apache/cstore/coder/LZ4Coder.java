package org.apache.cstore.coder;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.cstore.util.PerfLog;

import java.nio.ByteBuffer;

public class LZ4Coder
        implements BufferCoder<ByteBuffer>
{
    private ByteBuffer decodeBuffer;

    private final LZ4Factory factory = LZ4Factory.fastestInstance();

    public LZ4Coder(ByteBuffer decodeBuffer)
    {
        this.decodeBuffer = decodeBuffer;
    }

    @Override
    public ByteBuffer encode(ByteBuffer data)
    {
        PerfLog timer = PerfLog.LZ4_CODER_ENCODE.start();

        data.rewind();
        LZ4Compressor compressor = factory.fastCompressor();
        ByteBuffer out = ByteBuffer.allocate(data.capacity());
        compressor.compress(data, out);
        out.flip();

        timer.sum();
        return out.asReadOnlyBuffer();
    }

    @Override
    public ByteBuffer decode(ByteBuffer src)
    {
        PerfLog timer = PerfLog.LZ4_CODER_DECODE.start();
        src.rewind();
        decodeBuffer.clear();

        LZ4SafeDecompressor decompressor = factory.safeDecompressor();
        ByteBuffer out = decodeBuffer;
        decompressor.decompress(src, out);
        out.flip();

        timer.sum();
        return out.asReadOnlyBuffer();
    }
}

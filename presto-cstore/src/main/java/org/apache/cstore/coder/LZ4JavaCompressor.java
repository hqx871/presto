package org.apache.cstore.coder;

import io.airlift.compress.Compressor;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.nio.ByteBuffer;

public class LZ4JavaCompressor
        implements Compressor
{
    public static final LZ4JavaCompressor INSTANCE = new LZ4JavaCompressor();

    private static final LZ4Compressor LZ4_HIGH = LZ4Factory.fastestInstance().highCompressor();

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return LZ4_HIGH.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        return LZ4_HIGH.compress(input, inputOffset, inputLength, output, outputOffset, maxOutputLength);
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        LZ4_HIGH.compress(input, output);
    }
}

package org.apache.cstore.coder;

import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.nio.ByteBuffer;

public class LZ4JavaDecompressor
        implements Decompressor
{
    public static final LZ4JavaDecompressor INSTANCE = new LZ4JavaDecompressor();

    private static final LZ4SafeDecompressor LZ4_SAFE = LZ4Factory.fastestInstance().safeDecompressor();
    private static final LZ4FastDecompressor LZ4_FAST = LZ4Factory.fastestJavaInstance().fastDecompressor();

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        return LZ4_SAFE.decompress(input, inputOffset, inputLength, output, outputOffset, maxOutputLength);
    }

    @Override
    public void decompress(ByteBuffer input, ByteBuffer output)
            throws MalformedInputException
    {
        //LZ4_SAFE.decompress(input, output);
        LZ4_FAST.decompress(input, output);
    }
}

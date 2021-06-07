package org.apache.cstore.coder;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictTrainer;
import io.airlift.compress.Compressor;

import java.nio.ByteBuffer;

import static java.lang.Math.toIntExact;

public class ZstdJniCompressor
        implements Compressor
{
    private final byte[] dict;
    private final int level;

    public ZstdJniCompressor(byte[] dict, int level)
    {
        this.dict = dict;
        this.level = level;
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return toIntExact(Zstd.compressBound(uncompressedSize));
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        return toIntExact(Zstd.compressUsingDict(output, outputOffset, input, inputOffset, inputLength, dict, level));
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        Zstd.compress(input, output, dict, level);
    }

    public static byte[] train(boolean legacy, byte[] source)
    {
        ZstdDictTrainer trainer = new ZstdDictTrainer(1 << 20, 32 << 10);
        trainer.addSample(source);
        return trainer.trainSamples(false);
    }
}

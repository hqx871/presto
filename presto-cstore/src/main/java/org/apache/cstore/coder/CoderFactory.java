package org.apache.cstore.coder;

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;

public class CoderFactory
{
    public static final CoderFactory INSTANCE = new CoderFactory();

    public Compressor getCompressor(String type)
    {
        switch (type) {
            case "lz4":
                //return new Lz4Compressor();
                return LZ4JavaCompressor.INSTANCE;
            case "zstd":
                return new ZstdCompressor();
            case "snappy":
                return new SnappyCompressor();
            default:
        }
        throw new UnsupportedOperationException(type);
    }

    public Decompressor getDecompressor(String type)
    {
        switch (type) {
            case "lz4":
                //return new Lz4Decompressor();
                return LZ4JavaDecompressor.INSTANCE;
            case "zstd":
                return new ZstdDecompressor();
            case "snappy":
                return new SnappyDecompressor();
            default:
        }
        throw new UnsupportedOperationException(type);
    }
}

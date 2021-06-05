package org.apache.cstore.coder;

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;

public class CompressFactory
{
    public static final CompressFactory INSTANCE = new CompressFactory();

    private static final LZ4JavaCompressor lz4Compressor = LZ4JavaCompressor.INSTANCE;
    private static final LZ4JavaDecompressor lz4Decompressor = LZ4JavaDecompressor.INSTANCE;
    private static final ZstdCompressor zstdCompressor = new ZstdCompressor();
    private static final SnappyCompressor snappyCompressor = new SnappyCompressor();
    private static final ZstdDecompressor zstdDecompressor = new ZstdDecompressor();
    private static final SnappyDecompressor snappyDecompressor = new SnappyDecompressor();

    public Compressor getCompressor(String type)
    {
        switch (type) {
            case "lz4":
                //return new Lz4Compressor();
                return lz4Compressor;
            case "zstd":
                return zstdCompressor;
            case "snappy":
                return snappyCompressor;
            default:
        }
        throw new UnsupportedOperationException(type);
    }

    public Decompressor getDecompressor(String type)
    {
        switch (type) {
            case "lz4":
                //return new Lz4Decompressor();
                return lz4Decompressor;
            case "zstd":
                return zstdDecompressor;
            case "snappy":
                return snappyDecompressor;
            default:
        }
        throw new UnsupportedOperationException(type);
    }
}

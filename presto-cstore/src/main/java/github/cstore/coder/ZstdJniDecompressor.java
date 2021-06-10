package github.cstore.coder;

import com.github.luben.zstd.Zstd;
import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;

import java.nio.ByteBuffer;

import static java.lang.Math.toIntExact;

public class ZstdJniDecompressor
        implements Decompressor
{
    private final byte[] dict;

    public ZstdJniDecompressor(byte[] dict)
    {
        this.dict = dict;
    }

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        return toIntExact(Zstd.decompressUsingDict(output, outputOffset, input, inputOffset, inputLength, dict));
    }

    @Override
    public void decompress(ByteBuffer input, ByteBuffer output)
            throws MalformedInputException
    {
        Zstd.decompress(input, output, dict);
    }
}

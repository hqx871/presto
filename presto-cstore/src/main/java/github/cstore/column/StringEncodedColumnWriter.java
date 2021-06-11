package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.bitmap.Bitmap;
import github.cstore.bitmap.RoaringBitmapAdapter;
import github.cstore.dictionary.MutableTrieTree;
import github.cstore.io.StreamWriter;
import github.cstore.io.StreamWriterFactory;
import io.airlift.compress.Compressor;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class StringEncodedColumnWriter
        extends AbstractColumnWriter<String>
{
    private StreamWriter idWriter;
    private final MutableTrieTree dict;
    private final SortedMap<Integer, MutableRoaringBitmap> bitmaps;
    private int rowNum;
    private final boolean writeTreeDictionary;
    private final int pageSize;
    private final Compressor compressor;

    public StringEncodedColumnWriter(String name, int pageSize, Compressor compressor, MutableTrieTree dict,
            StreamWriterFactory writerFactory, boolean writeTreeDictionary, boolean delete)
    {
        super(name, writerFactory, delete);
        this.writeTreeDictionary = writeTreeDictionary;
        this.pageSize = pageSize;
        this.compressor = compressor;
        this.idWriter = writerFactory.createWriter(name + ".id", true);
        this.dict = dict;

        this.bitmaps = new TreeMap<>();
        this.bitmaps.put(0, new MutableRoaringBitmap());
        this.rowNum = 0;
    }

    @Override
    public int write(String value)
    {
        int id = dict.encode(value);
        idWriter.putInt(id);
        MutableRoaringBitmap bitmap = bitmaps.computeIfAbsent(id, k -> new MutableRoaringBitmap());
        bitmap.add(rowNum++);
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        flush();
        if (idWriter != null) {
            idWriter.close();
        }
        idWriter = null;
        super.close();
    }

    @Override
    public void doFlush()
            throws IOException
    {
        idWriter.flush();

        int[] newIds = dict.sortValue();
        int sstSize = dict.writeSst(streamWriter, name + ".dict", writerFactory);
        streamWriter.putInt(sstSize);
        int dataSize = writeData(pageSize, compressor, streamWriter, dict.maxEncodeId(), newIds);
        streamWriter.putInt(dataSize);
        int bitmapSize = writeBitmap(streamWriter, newIds);
        streamWriter.putInt(bitmapSize);
        streamWriter.flush();

        if (writeTreeDictionary) {
            StreamWriter treeWriter = writerFactory.createWriter(name + ".dict", delete);
            dict.writeTrieTree(treeWriter);
            treeWriter.close();
        }
    }

    private int writeBitmap(StreamWriter streamWriter, int[] newIds)
            throws IOException
    {
        BinaryOffsetColumnWriter<Bitmap> bitmapWriter = new BinaryOffsetColumnWriter<>(name + ".bitmap", writerFactory,
                BitmapColumnReader.coder, false);

        SortedMap<Integer, MutableRoaringBitmap> newBitmaps = new TreeMap<>();
        bitmaps.forEach((oldId, bitmap) -> {
            int newId = newIds[oldId];
            newBitmaps.put(newId, bitmap);
        });
        for (Entry<Integer, MutableRoaringBitmap> valueBitmap : newBitmaps.entrySet()) {
            MutableRoaringBitmap bitmap = valueBitmap.getValue();
            bitmapWriter.write(new RoaringBitmapAdapter(bitmap));
        }
        bitmapWriter.flush();
        int size = bitmapWriter.appendTo(streamWriter);
        bitmapWriter.close();
        return size;
    }

    private int writeData(int pageSize, Compressor compressor, StreamWriter output, int ceilId, int[] newIds)
            throws IOException
    {
        IntBuffer ids = idWriter.map().asIntBuffer();
        int size = 0;
        if (ceilId <= Byte.MAX_VALUE) {
            output.putByte(ColumnEncodingId.PLAIN_BYTE);
            ByteColumnPlainWriter bytePlainWriter = new ByteColumnPlainWriter(name + ".id-plain", writerFactory, true);
            ChunkColumnWriter<Byte> pageWriter = new ChunkColumnWriter<>(name + ".id", pageSize, compressor, writerFactory, bytePlainWriter, delete);

            while (ids.hasRemaining()) {
                pageWriter.write((byte) newIds[ids.get()]);
            }
            pageWriter.flush();
            size = Byte.BYTES + pageWriter.appendTo(output);
            pageWriter.close();
        }
        else if (ceilId <= Short.MAX_VALUE) {
            output.putByte(ColumnEncodingId.PLAIN_SHORT);
            ShortColumnPlainWriter shortVectorWriter = new ShortColumnPlainWriter(name + ".id-plain", writerFactory, true);
            ChunkColumnWriter<Short> pageWriter = new ChunkColumnWriter<>(name + ".id", pageSize, compressor, writerFactory, shortVectorWriter, delete);
            while (ids.hasRemaining()) {
                pageWriter.write((short) newIds[ids.get()]);
            }
            pageWriter.flush();
            size = Byte.BYTES + pageWriter.appendTo(output);
            pageWriter.close();
        }
        else {
            output.putByte(ColumnEncodingId.PLAIN_INT);
            IntColumnPlainWriter intVectorWriter = new IntColumnPlainWriter(name + ".id-plain", writerFactory, true);
            ChunkColumnWriter<Integer> pageWriter = new ChunkColumnWriter<>(name + ".id", pageSize, compressor, writerFactory, intVectorWriter, delete);
            while (ids.hasRemaining()) {
                pageWriter.write(newIds[ids.get()]);
            }
            pageWriter.flush();
            size = Byte.BYTES + pageWriter.appendTo(output);
            pageWriter.close();
        }
        return size;
    }

    @Override
    public String readBlockValue(Block src, int position)
    {
        return src.getSlice(position, 0, src.getSliceLength(position)).toStringUtf8();
    }

    @Override
    public int writeNull()
    {
        return write(null);
    }
}

package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.bitmap.Bitmap;
import github.cstore.bitmap.RoaringBitmapAdapter;
import github.cstore.dictionary.MutableStringDictionary;
import github.cstore.io.MemoryStreamWriterFactory;
import github.cstore.io.StreamWriter;
import github.cstore.io.StreamWriterFactory;
import io.airlift.compress.Compressor;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class StringEncodedColumnWriter
        extends AbstractColumnWriter<String>
{
    private StreamWriter idWriter;
    private final MutableStringDictionary dict;
    private final SortedMap<Integer, MutableRoaringBitmap> bitmaps;
    private int rowNum;
    private final short pageRowCount;
    private final Compressor compressor;
    private final StreamWriterFactory writerFactory;
    private final boolean writeIndex;

    public StringEncodedColumnWriter(String name, short pageRowCount, Compressor compressor, MutableStringDictionary dict,
            StreamWriter valueStreamWriter, StreamWriterFactory writerFactory, boolean writeIndex, boolean delete)
    {
        super(name, valueStreamWriter, delete);
        this.writerFactory = writerFactory;
        this.pageRowCount = pageRowCount;
        this.compressor = compressor;
        this.idWriter = writerFactory.createWriter(name + ".id", true);
        this.dict = dict;

        this.bitmaps = new TreeMap<>();
        this.bitmaps.put(0, new MutableRoaringBitmap());
        this.rowNum = 0;
        this.writeIndex = writeIndex;
    }

    @Override
    public int doWrite(String value)
    {
        int id = dict.encodeValue(value);
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
        int sstSize = dict.writeSst(valueStreamWriter, name + ".dict", new MemoryStreamWriterFactory());
        valueStreamWriter.putInt(sstSize);
        int dataSize = writeData(pageRowCount, compressor, valueStreamWriter, dict.getMaxId(), newIds);
        valueStreamWriter.putInt(dataSize);
        if (writeIndex) {
            int bitmapSize = writeBitmap(valueStreamWriter, newIds);
            valueStreamWriter.putInt(bitmapSize);
        }

        valueStreamWriter.flush();
        super.doFlush();
    }

    private int writeBitmap(StreamWriter streamWriter, int[] newIds)
            throws IOException
    {
        BinaryOffsetColumnWriter<Bitmap> bitmapWriter = new BinaryOffsetColumnWriter<>(name + ".bitmap", writerFactory.createWriter(name + ".bitmap", true), writerFactory,
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
        ByteBuffer bitmapBuffer = bitmapWriter.mapBuffer();
        int size = bitmapBuffer.remaining();
        streamWriter.putByteBuffer(bitmapBuffer);
        bitmapWriter.close();
        return size;
    }

    private int writeData(short pageRowCount, Compressor compressor, StreamWriter output, int ceilId, int[] newIds)
            throws IOException
    {
        IntBuffer ids = idWriter.toByteBuffer().asIntBuffer();
        int size = 0;
        StreamWriterFactory memoryStreamWriterFactory = new MemoryStreamWriterFactory();
        if (ceilId <= Byte.MAX_VALUE) {
            output.putByte(ColumnEncodingId.PLAIN_BYTE);
            ByteColumnPlainWriter bytePlainWriter = new ByteColumnPlainWriter(name + ".id-plain", memoryStreamWriterFactory.createWriter(name + ".id-plain", true), true);
            ColumnChunkZipWriter<Byte> pageWriter = new ColumnChunkZipWriter<>(name + ".id", pageRowCount, compressor, writerFactory.createWriter(name + ".id", true), memoryStreamWriterFactory, bytePlainWriter, delete);
            pageWriter.setup();
            while (ids.hasRemaining()) {
                pageWriter.write((byte) newIds[ids.get()]);
            }
            ByteBuffer pageBuffer = pageWriter.mapBuffer();
            size = Byte.BYTES + pageBuffer.remaining();
            output.putByteBuffer(pageBuffer);
            pageWriter.close();
        }
        else if (ceilId <= Short.MAX_VALUE) {
            output.putByte(ColumnEncodingId.PLAIN_SHORT);
            ShortColumnPlainWriter shortVectorWriter = new ShortColumnPlainWriter(name + ".id-plain", memoryStreamWriterFactory.createWriter(name + ".id-plain", true), true);
            ColumnChunkZipWriter<Short> pageWriter = new ColumnChunkZipWriter<>(name + ".id", pageRowCount, compressor, writerFactory.createWriter(name + ".id", true), memoryStreamWriterFactory, shortVectorWriter, delete);
            pageWriter.setup();
            while (ids.hasRemaining()) {
                pageWriter.write((short) newIds[ids.get()]);
            }
            ByteBuffer pageBuffer = pageWriter.mapBuffer();
            size = Byte.BYTES + pageBuffer.remaining();
            output.putByteBuffer(pageBuffer);
            pageWriter.close();
        }
        else {
            output.putByte(ColumnEncodingId.PLAIN_INT);
            IntColumnPlainWriter intVectorWriter = new IntColumnPlainWriter(name + ".id-plain", memoryStreamWriterFactory.createWriter(name + ".id-plain", true), true);
            ColumnChunkZipWriter<Integer> pageWriter = new ColumnChunkZipWriter<>(name + ".id", pageRowCount, compressor, writerFactory.createWriter(name + ".id", true), memoryStreamWriterFactory, intVectorWriter, delete);
            pageWriter.setup();
            while (ids.hasRemaining()) {
                pageWriter.write(newIds[ids.get()]);
            }
            ByteBuffer pageBuffer = pageWriter.mapBuffer();
            size = Byte.BYTES + pageBuffer.remaining();
            output.putByteBuffer(pageBuffer);
            pageWriter.close();
        }
        return size;
    }

    @Override
    public int writeNull()
    {
        return doWrite(null);
    }

    @Override
    public String readValue(Block src, int position)
    {
        if (src.isNull(position)) {
            return null;
        }
        return src.getSlice(position, 0, src.getSliceLength(position)).toStringUtf8();
    }
}

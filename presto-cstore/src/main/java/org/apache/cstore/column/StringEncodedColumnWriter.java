package org.apache.cstore.column;

import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.bitmap.RoaringBitmapAdapter;
import org.apache.cstore.dictionary.MutableTrieTree;
import org.apache.cstore.io.CStoreColumnWriter;
import org.apache.cstore.io.OutputStreamWriter;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.util.IOUtil;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class StringEncodedColumnWriter
        implements CStoreColumnWriter<String>
{
    private DataOutputStream idStream;
    private DataOutputStream bitmapStream;
    private DataOutputStream dataStream;

    private final MutableTrieTree dict;
    private final SortedMap<Integer, MutableRoaringBitmap> bitmaps;
    private final VectorWriterFactory writerFactor;
    private int rowNum;
    private File idFile;
    private final File dataFile;

    public StringEncodedColumnWriter(MutableTrieTree dict, VectorWriterFactory writerFactor)
    {
        this.writerFactor = writerFactor;
        this.idFile = writerFactor.newFile(writerFactor.getName() + ".id");
        File bitmapFile = writerFactor.newFile(writerFactor.getName() + ".bitmap");
        this.idStream = IOUtil.openFileDataStream(idFile);

        this.bitmapStream = IOUtil.openFileDataStream(bitmapFile);
        this.dict = dict;

        this.bitmaps = new TreeMap<>();
        this.bitmaps.put(0, new MutableRoaringBitmap());
        this.rowNum = 0;
        this.dataFile = writerFactor.newFile(writerFactor.getName() + ".bin");
        this.dataStream = IOUtil.openFileDataStream(dataFile);
    }

    @Override
    public int write(String value)
    {
        try {
            int id = dict.encode(value);
            idStream.writeInt(id);
            MutableRoaringBitmap bitmap = bitmaps.computeIfAbsent(id, k -> new MutableRoaringBitmap());
            bitmap.add(rowNum++);
            return 0;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        flush();
        if (idStream != null) {
            idStream.close();
        }
        idStream = null;
        if (bitmapStream != null) {
            bitmapStream.close();
        }
        bitmapStream = null;
        if (idFile != null) {
            idFile.delete();
        }
        idFile = null;
        if (dataStream != null) {
            dataStream.close();
        }
        dataStream = null;
    }

    @Override
    public int flushTo(StreamWriter output)
            throws IOException
    {
        int[] newIds = dict.sortValue();
        int dictSize = dict.write(output, writerFactor);
        output.putInt(dictSize);
        int dataSize = writeData(output, idFile, dict.maxEncodeId(), newIds);
        output.putInt(dataSize);

        StreamWriter bitmapWriter = new OutputStreamWriter(bitmapStream);
        writeBitmap(bitmapWriter, newIds);
        bitmapStream.flush();

        return dictSize + dataSize + Integer.BYTES * 2;
    }

    @Override
    public void flush()
            throws IOException
    {
        idStream.flush();

        StreamWriter streamWriter = new OutputStreamWriter(dataStream);
        flushTo(streamWriter);
        dataStream.flush();
    }

    private int writeBitmap(StreamWriter mergeStream, int[] newIds)
            throws IOException
    {
        BinaryOffsetWriter<Bitmap> bitmapWriter = new BinaryOffsetWriter<>(
                new VectorWriterFactory(writerFactor.getDir(), writerFactor.getName() + ".bitmap"),
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
        int size = bitmapWriter.flushTo(mergeStream);
        bitmapWriter.close();
        return size;
    }

    private int writeData(StreamWriter output, File tmpDataFile, int ceilId, int[] newIds)
            throws IOException
    {
        IntBuffer ints = IOUtil.mapFile(tmpDataFile, MapMode.READ_ONLY).asIntBuffer();

        int size = 0;
        if (ceilId <= Byte.MAX_VALUE) {
            output.putByte(ColumnEncodingId.PLAIN_BYTE);
            ByteColumnPlainWriter bytePlainWriter = new ByteColumnPlainWriter(new VectorWriterFactory(writerFactor.getDir(), writerFactor.getName() + ".id"), true);
            while (ints.hasRemaining()) {
                bytePlainWriter.write((byte) newIds[ints.get()]);
            }
            size = Byte.BYTES + bytePlainWriter.flushTo(output);
            bytePlainWriter.close();
        }
        else if (ceilId <= Short.MAX_VALUE) {
            output.putByte(ColumnEncodingId.PLAIN_SHORT);
            ShortColumnWriter shortVectorWriter = new ShortColumnWriter(new VectorWriterFactory(writerFactor.getDir(), writerFactor.getName() + ".id"), true);
            while (ints.hasRemaining()) {
                shortVectorWriter.write((short) newIds[ints.get()]);
            }
            size = Byte.BYTES + shortVectorWriter.flushTo(output);
            shortVectorWriter.close();
        }
        else {
            output.putByte(ColumnEncodingId.PLAIN_INT);
            IntColumnWriter intVectorWriter = new IntColumnWriter(new VectorWriterFactory(writerFactor.getDir(), writerFactor.getName() + ".id"), true);
            while (ints.hasRemaining()) {
                intVectorWriter.write(newIds[ints.get()]);
            }
            size = Byte.BYTES + intVectorWriter.flushTo(output);
            intVectorWriter.close();
        }
        return size;
    }
}

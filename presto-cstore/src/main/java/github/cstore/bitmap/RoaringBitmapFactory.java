package github.cstore.bitmap;

import com.google.common.base.Preconditions;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.util.ArrayList;
import java.util.List;

public class RoaringBitmapFactory
        implements BitmapFactory
{
    @Override
    public Bitmap create(int[] value, int offset, int length)
    {
        MutableRoaringBitmap mutable = new MutableRoaringBitmap();
        for (int i = 0; i < length; i++) {
            mutable.add(value[offset + i]);
        }
        return new RoaringBitmapAdapter(length, mutable.toImmutableRoaringBitmap());
    }

    @Override
    public Bitmap and(List<Bitmap> others)
    {
        Preconditions.checkArgument(others.size() > 0);
        if (others.size() == 1) {
            return others.get(0);
        }
        List<ImmutableRoaringBitmap> bitmaps = new ArrayList<>(others.size());
        others.forEach(other -> bitmaps.add(other.unwrap()));
        ImmutableRoaringBitmap and = BufferFastAggregation.and(bitmaps.iterator()).toImmutableRoaringBitmap();
        return new RoaringBitmapAdapter(others.get(0).getCapacity(), and);
    }

    @Override
    public Bitmap or(List<Bitmap> others)
    {
        Preconditions.checkArgument(others.size() > 0);
        if (others.size() == 1) {
            return others.get(0);
        }
        List<ImmutableRoaringBitmap> bitmaps = new ArrayList<>(others.size());
        others.forEach(other -> bitmaps.add(other.unwrap()));
        ImmutableRoaringBitmap or = BufferFastAggregation.or(bitmaps.iterator()).toImmutableRoaringBitmap();
        return new RoaringBitmapAdapter(others.get(0).getCapacity(), or);
    }
}

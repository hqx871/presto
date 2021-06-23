package github.cstore.bitmap;

import java.util.List;

public interface BitmapFactory
{
    Bitmap create(int[] value, int offset, int length);

    Bitmap and(List<Bitmap> others);

    Bitmap or(List<Bitmap> others);
}

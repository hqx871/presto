package github.cstore.dictionary;

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class MapDictionary
        extends MutableStringDictionary
{
    private final Object2IntMap<String> dictionary;
    private final List<String> nonNullValues;
    private int nextId;
    private int nullId;

    public MapDictionary()
    {
        this.dictionary = new Object2IntOpenHashMap<>();
        this.nonNullValues = new ArrayList<>();
        this.nextId = 1;
        this.nullId = -1;
    }

    @Override
    public int encode(String value)
    {
        if (value == null) {
            nullId = 0;
            return 0;
        }
        return dictionary.computeIfAbsent(value, k -> {
            nonNullValues.add(value);
            return nextId++;
        });
    }

    @Override
    public int[] sortValue()
    {
        Integer[] ids = new Integer[nonNullValues.size() + 1];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = i;
        }
        Arrays.sort(ids, 1, ids.length, (a, b) -> Objects.compare(nonNullValues.get(a - 1), nonNullValues.get(b - 1), String::compareTo));
        List<String> unSortValues = ImmutableList.copyOf(nonNullValues);
        nonNullValues.clear();
        for (int i = 1; i < ids.length; i++) {
            int id = ids[i];
            String value = unSortValues.get(id - 1);
            nonNullValues.add(value);
            dictionary.put(value, i);
        }
        return Arrays.stream(ids).mapToInt(i -> i).toArray();
    }

    @Override
    public List<String> getNonNullValues()
    {
        return nonNullValues;
    }

    @Override
    public int lookupId(String value)
    {
        if (value == null) {
            return nullId == 0 ? 0 : -1;
        }
        return dictionary.getInt(value);
    }

    @Override
    public String lookupValue(int id)
    {
        if (id == 0) {
            return null;
        }
        return nonNullValues.get(id);
    }

    @Override
    public int count()
    {
        return nextId - 1 + (nullId == 0 ? 1 : 0);
    }

    @Override
    public int getMaxId()
    {
        return nextId;
    }
}

package github.cstore.dictionary;

public class TrieTreeNode
        implements Comparable<TrieTreeNode>
{
    private int id;
    private char[] value;
    private TrieTreeNode[] children;

    public TrieTreeNode(int id, char[] value, TrieTreeNode[] children)
    {
        this.id = id;
        this.value = value;
        this.children = children;
    }

    @Override
    public int compareTo(TrieTreeNode other)
    {
        int n = Math.min(this.getValue().length, other.getValue().length);
        for (int i = 0; i < n; i++) {
            int compared = this.getValue()[i] - other.getValue()[i];
            if (compared != 0) {
                return compared;
            }
        }
        return this.getValue().length - other.getValue().length;
    }

    public char[] getValue()
    {
        return value;
    }

    public void setValue(char[] value)
    {
        this.value = value;
    }

    public int getId()
    {
        return id;
    }

    public void setId(int id)
    {
        this.id = id;
    }

    public TrieTreeNode[] getChildren()
    {
        return children;
    }

    public void setChildren(TrieTreeNode[] children)
    {
        this.children = children;
    }
}

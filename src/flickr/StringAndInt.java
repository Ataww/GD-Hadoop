package flickr;

import java.util.Comparator;

/**
 * Created by Ataww on 08/12/2016.
 */
public class StringAndInt implements Comparable<StringAndInt> {
    private String tag;
    private int count;

    public StringAndInt(String tag, int count) {
        this.tag = tag;
        this.count = count;
    }

    public void increment() {
        count++;
    }

    public String getTag() {
        return tag;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public int compareTo(StringAndInt o) {
        return Integer.compare(this.count,o.count);
    }
}

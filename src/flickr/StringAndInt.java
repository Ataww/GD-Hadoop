package flickr;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Ataww on 08/12/2016.
 */
public class StringAndInt implements WritableComparable<StringAndInt> {
    private String tag;
    private int count;

    public StringAndInt(){}

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
        if (this.tag.equals(o.getTag())) {
            return Integer.compare(o.count, this.count);
        } else {
            return this.tag.compareTo(o.getTag());
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.tag);
        dataOutput.writeInt(this.count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.tag = dataInput.readUTF();
        this.count = dataInput.readInt();
    }
}

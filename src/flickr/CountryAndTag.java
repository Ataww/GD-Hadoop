package flickr;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by julien on 18/12/16.
 */
public class CountryAndTag  implements WritableComparable<CountryAndTag> {

    private String country;
    private String tag;

    public CountryAndTag(){}

    public CountryAndTag(String country, String tag) {
        this.country = country;
        this.tag = tag;
    }

    public String getCountry() {
        return country;
    }

    public String getTag() {
        return tag;
    }

    @Override
    public int compareTo(CountryAndTag o) {
        if (this.country.equals(o.getCountry())) {
            return this.tag.compareTo(o.getTag());
        } else {
            return this.country.compareTo(o.getCountry());
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.country);
        dataOutput.writeUTF(this.tag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.country = dataInput.readUTF();
        this.tag = dataInput.readUTF();
    }
}

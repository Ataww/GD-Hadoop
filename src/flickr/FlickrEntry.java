package flickr;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static flickr.Country.getCountryAt;

/**
 * Created by Ataww on 08/12/2016.
 */
public class FlickrEntry {

    private String[] data;
    private String country;

    public FlickrEntry(String line) {
        data = line.split("\\t");
        if (data.length != 23) {
            throw new IllegalArgumentException("expected 23, got " + data.length);
        }
    }

    public String[] getTags() {
        return data[8].split(",");
    }

    public String getCountry() {
        if(country == null) {
            try {
                country = getCountryAt(Double.valueOf(data[11]), Double.valueOf(data[10])).toString();
            } catch (NullPointerException e) {
                System.out.println("APPROPRIATOR");
                return null;
            }
        }
        return country;
    }

    public static void main(String args[]) throws IOException {
        List<FlickrEntry> entries = new LinkedList<>();
        FileReader fr = new FileReader(args[0]);
        BufferedReader br = new BufferedReader(fr);
        String s;
        while ((s = br.readLine()) != null) {
            entries.add(new FlickrEntry(s));
        }
        System.out.println(entries.size());
    }
}

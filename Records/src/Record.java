import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Record implements WritableComparable<Record> {
    IntWritable albumCount;
    private Set<String> genres;
    // There is no SetWritable type - unfortunately I needed to use this:
    ArrayWritable genresArr;

    public Record() {
        this.albumCount = new IntWritable(0);
        this.genres = new HashSet<>();
        this.genresArr = new ArrayWritable(Text.class);
    }

    public Record(int albumCount, String genre) {
        this();
        setAlbumCount(albumCount);
        addGenre(genre);
    }

    public void setAlbumCount(int albumCount) {
        this.albumCount.set(albumCount);
    }

    public void addGenre(String genre){
        genres.add(genre);
        Text[] textArray = genres.stream()
                .map(Text::new)
                .toArray(Text[]::new);
        genresArr = new ArrayWritable(Text.class, textArray);
    }

    public void merge(Record other) {
        this.albumCount.set(this.albumCount.get() + other.albumCount.get());
        for (Writable element : other.genresArr.get()) {
            Text text = (Text) element;
            String genre = text.toString();
            this.genres.add(genre);
        }
        Text[] textArray = genres.stream()
                .map(Text::new)
                .toArray(Text[]::new);
        genresArr = new ArrayWritable(Text.class, textArray);
    }

    @Override
    public int compareTo(Record o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        albumCount.write(dataOutput);
        genresArr.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        albumCount.readFields(dataInput);
        genresArr.readFields(dataInput);
    }

    @Override
    public String toString() {
        return albumCount.get() + ",[" + String.join(",", Arrays.asList(genresArr.toStrings())) + "]";
    }
}
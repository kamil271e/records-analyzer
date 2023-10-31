import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Record implements WritableComparable<Record> {
    IntWritable albumCount;
    Set<String> genres;
    public Record() {
        this.albumCount = new IntWritable(0);
        this.genres = new HashSet<>();
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
        this.genres.add(genre);
    }

    public void merge(int albumCount, Set<String> genres) {
        setAlbumCount(albumCount);
        this.genres.addAll(genres);
    }

    @Override
    public int compareTo(Record o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        albumCount.write(dataOutput);
        Text.writeString(dataOutput, genres.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        albumCount.readFields(dataInput);
        String genreList = Text.readString(dataInput);
        genreList = genreList.replaceAll("\\[|\\]", "");
        genreList = genreList.replaceAll(",,", ",");
        String[] genreArr = genreList.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        this.genres = new HashSet<>(Arrays.asList(genreArr));
    }

    @Override
    public String toString() {
        return albumCount.get() + ":" + String.join(",", genres);
    }
}

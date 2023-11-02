import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecordsKey implements WritableComparable<RecordsKey> {
    private int labelId;
    private int artistId;
    private String artistName;
    private int decade;

    public RecordsKey() {}

    public RecordsKey(int labelId, int artistId, String artistName, int decade){
        this.labelId = labelId;
        this.artistId = artistId;
        this.artistName = artistName;
        this.decade = decade;
    }
    @Override
    public int compareTo(RecordsKey o) {
        if (this.labelId > o.labelId)
            return 1;
        if (this.labelId < o.labelId)
            return -1;
        if (this.artistId > o.artistId)
            return 1;
        if(this.artistId < o.artistId)
            return -1;
        return Integer.compare(this.decade, o.decade);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.labelId);
        dataOutput.writeInt(this.artistId);
        Text.writeString(dataOutput, this.artistName);
        dataOutput.writeInt(this.decade);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.labelId = dataInput.readInt();
        this.artistId = dataInput.readInt();
        this.artistName = Text.readString(dataInput);
        this.decade = dataInput.readInt();
    }

    @Override
    public String toString() {
        return this.labelId + "\t" + this.artistId + "\t" + this.artistName + "\t" + this.decade;
    }
}

package neu;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortClass implements WritableComparable<SortClass> {
    public SortClass(Float pr) {
        this.pr = pr;
    }

    private Float pr;



    @Override
    public int compareTo(SortClass o) {
        return this.pr - o.getPr() > 0? 1:0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(pr);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pr = dataInput.readFloat();
    }

    public Float getPr() {
        return pr;
    }

    public void setPr(Float pr) {
        this.pr = pr;
    }


}

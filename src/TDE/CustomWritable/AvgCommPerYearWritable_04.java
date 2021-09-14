package TDE.CustomWritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AvgCommPerYearWritable_04 implements WritableComparable<AvgCommPerYearWritable_04> {

    private int count;
    private double commValue;

    public AvgCommPerYearWritable_04() {

    }

    public AvgCommPerYearWritable_04(int count, double commValue) {
        this.count = count;
        this.commValue = commValue;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getCommValue() {
        return commValue;
    }

    public void setCommValue(double commValue) {
        this.commValue = commValue;
    }

    @Override
    public int compareTo(AvgCommPerYearWritable_04 o) {
        if (this.hashCode() < o.hashCode()) {
            return -1;
        } else if (this.hashCode() > o.hashCode()) {
            return +1;
        } else {
            return 0;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, commValue);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeDouble(commValue);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readInt();
        commValue = dataInput.readDouble();
    }

}

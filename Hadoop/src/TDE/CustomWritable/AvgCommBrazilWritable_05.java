package TDE.CustomWritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AvgCommBrazilWritable_05 implements  WritableComparable<AvgCommBrazilWritable_05> {

    private double value;
    private int counter;

    public AvgCommBrazilWritable_05() {

    }

    public AvgCommBrazilWritable_05(double value, int counter) {
        this.value = value;
        this.counter = counter;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, counter);
    }

    @Override
    public int compareTo(AvgCommBrazilWritable_05 o) {
        if(this.hashCode() < o.hashCode()){
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return +1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(value);
        dataOutput.writeInt(counter);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        value = dataInput.readDouble();
        counter = dataInput.readInt();
    }

}

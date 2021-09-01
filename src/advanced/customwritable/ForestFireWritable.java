package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ForestFireWritable implements WritableComparable<ForestFireWritable> {

    private double temp;
    private double wind;

    public ForestFireWritable() {

    }

    public ForestFireWritable(double temp, double wind) {
        this.temp = temp;
        this.wind = wind;
    }

    public double getTemp() {
        return temp;
    }

    public void setTemp(double temp) {
        this.temp = temp;
    }

    public double getWind() {
        return wind;
    }

    public void setWind(double wind) {
        this.wind = wind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(temp, wind);
    }

    @Override
    public int compareTo(ForestFireWritable o) {
        if (this.hashCode() < o.hashCode()) {
            return -1;
        }
        else if (this.hashCode() > o.hashCode()) {
            return +1;
        }
        else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(temp);
        dataOutput.writeDouble(wind);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        temp = dataInput.readDouble();
        wind = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "{"+
                "temperatura = " + temp +
                ", vento = " + wind +
                "}";
    }

}

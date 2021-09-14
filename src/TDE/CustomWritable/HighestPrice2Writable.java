package TDE.CustomWritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class HighestPrice2Writable implements WritableComparable<HighestPrice2Writable> {

    private double unit;
    private double price;
//    private double max;

    public HighestPrice2Writable() {
    }

    public HighestPrice2Writable(double unit, double price) {
        this.unit = unit;
        this.price = price;
    }

//    public SeisWritable(double unit, double price, double maxPricePorUnity) {
//        this.unit = unit;
//        this.price = price;
//        this.max = maxPricePorUnity;
//    }

//    public double getMax() {
//        return max;
//    }
//
//    public void setMax(double max) {
//        this.max = max;
//    }

    public double getUnit() {
        return unit;
    }

    public void setUnit(double unit) {
        this.unit = unit;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }


    @Override
    public int hashCode() {
        return Objects.hash(price, unit);
    }

    @Override
    public int compareTo(HighestPrice2Writable o) {
        if (this.hashCode() < o.hashCode()) return -1;
        else if (this.hashCode() > o.hashCode()) return +1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(unit);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        unit = dataInput.readDouble();
        price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "{" +
                "quantity=" + unit +
                ", price=" + price +
//                ",  highest price per unit type and year=" + max +
                '}';
    }
}
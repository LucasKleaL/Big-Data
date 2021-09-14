package TDE.CustomWritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class HighestPriceWritable implements WritableComparable<HighestPriceWritable> {

    private String commodity;
    private double price;

    public HighestPriceWritable() {
    }

    public HighestPriceWritable(String commodity, double price) {
        this.commodity = commodity;
        this.price = price;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, price);
    }

    @Override
    public int compareTo(HighestPriceWritable o) {
        if (this.hashCode() < o.hashCode()) return -1;
        else if (this.hashCode() > o.hashCode()) return +1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(commodity);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "{" +
                "commodity=" + commodity +
                ", price=" + price +
                '}';
    }

}

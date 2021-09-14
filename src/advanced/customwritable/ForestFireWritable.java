package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


public class ForestFireWritable implements WritableComparable<ForestFireWritable> {

    private double temperatura;
    private double vento;

    public ForestFireWritable() {
    }

    public ForestFireWritable(double temperatura, double vento) {
        this.temperatura = temperatura;
        this.vento = vento;
    }

    public double getTemperatura() {
        return temperatura;
    }

    public void setTemperatura(double temperatura) {
        this.temperatura = temperatura;
    }

    public double getVento() {
        return vento;
    }

    public void setVento(double vento) {
        this.vento = vento;
    }


    @Override
    public int hashCode() {
        return Objects.hash(vento, temperatura);
    }

    @Override
    public int compareTo(ForestFireWritable o) {
        if (this.hashCode() < o.hashCode()) return -1;
        else if (this.hashCode() > o.hashCode()) return +1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(temperatura);
        dataOutput.writeDouble(vento);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        temperatura = dataInput.readDouble();
        vento = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "{" +
                "temperatura=" + temperatura +
                ", vento=" + vento +
                '}';
    }
}

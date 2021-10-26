package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


//  Todo Writable precisa ser um Java Bean
//  Java Bean:
//  1 - Atributos devem ser privados
//  2 - Construtor vazio
//  3 - Gets e sets para todos os atributos

public class FireAvgTempWritable implements  WritableComparable<FireAvgTempWritable>  {

    private int n;
    private double temp;

    public FireAvgTempWritable() {

    }

    public FireAvgTempWritable(int n, double temp) {
        this.n = n;
        this.temp = temp;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public double getTemp() {
        return temp;
    }

    public void setTemp(double temp) {
        this.temp = temp;
    }

    @Override
    //  A função compareTo é universal para a maioria das rotinas
    public int compareTo(FireAvgTempWritable o) {
        if (this.hashCode() < o.hashCode()) {
            return -1;
        }
        else if (this.hashCode() > o.hashCode()){
            return +1;
        }
        else {
            return 0;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(n, temp);
    }

    //  A ordem dos atributos do write e readFields devem ser sempre a mesma

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(n); //enviando a ocorrencia do objeto
        dataOutput.writeDouble(temp); //enviando a temperatura do objeto
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        n = dataInput.readInt();
        temp = dataInput.readDouble();
    }
}

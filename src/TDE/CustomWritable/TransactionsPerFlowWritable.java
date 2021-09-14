package TDE.CustomWritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TransactionsPerFlowWritable implements WritableComparable<TransactionsPerFlowWritable>{

    private String flow;
    private int ocorrencia;

    public TransactionsPerFlowWritable() {

    }

    public TransactionsPerFlowWritable(String flow, int ocorrencia) {
        this.flow = flow;
        this.ocorrencia = ocorrencia;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    public int getOcorrencia() {
        return ocorrencia;
    }

    public void setOcorrencia(int ocorrencia) {
        this.ocorrencia = ocorrencia;
    }

    @Override
    public int hashCode() {
        return Objects.hash(flow, ocorrencia);
    }

    @Override
    public int compareTo(TransactionsPerFlowWritable o) {
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
        dataOutput.writeChars(flow);
        dataOutput.writeInt(ocorrencia);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        flow = dataInput.readLine();
        ocorrencia = dataInput.readInt();
    }

    /*
    @Override
    public String toString() {
        return "{"+
                "temperatura = " + temp +
                ", vento = " + wind +
                "}";
    }
    */

}

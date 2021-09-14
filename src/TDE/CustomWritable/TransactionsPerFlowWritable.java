package TDE.CustomWritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TransactionsPerFlowWritable implements WritableComparable<TransactionsPerFlowWritable>{

    private String year;
    private String flow;

    public TransactionsPerFlowWritable() {

    }

    public TransactionsPerFlowWritable(String year, String flow) {
        this.year = year;
        this.flow = flow;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, flow);
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
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(flow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readUTF();
        flow = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "{ "+
                "flow_type: " + this.flow +
                " - year: " + this.year +
                " }" + " total_transactions: ";
    }

}

package TDE.CustomWritable;

import advanced.entropy.BaseQtdWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MostCommercializedWritable implements WritableComparable<MostCommercializedWritable> {

    private String commodity;
    private long qtd;

    public MostCommercializedWritable() {

    }

    public MostCommercializedWritable(String commodity, long qtd) {
        this. commodity = commodity;
        this.qtd = qtd;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public long getQtd() {
        return qtd;
    }

    public void setQtd(long qtd) {
        this.qtd = qtd;
    }

    @Override
    public int compareTo(MostCommercializedWritable o) {
        if (this.hashCode() < o.hashCode()) return -1;
        else if (this.hashCode() > o.hashCode()) return +1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(commodity);
        dataOutput.writeLong(qtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        qtd = dataInput.readLong();
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, qtd);
    }

}

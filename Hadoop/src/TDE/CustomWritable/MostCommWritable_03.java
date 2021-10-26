package TDE.CustomWritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MostCommWritable_03 implements WritableComparable<MostCommWritable_03>  {
    private String ano;
    private Double qtde;
    public MostCommWritable_03() {
    }

    public MostCommWritable_03(Double qtde, String ano) {
        this.qtde = qtde;
        this.ano = ano;
    }

    public String getAno() {
        return ano;
    }

    public void setAno(String ano) {
        this.ano = ano;
    }

    public Double getQtde() {
        return qtde;
    }

    public void setQtde(Double qtde) {
        this.qtde = qtde;
    }

    @Override
    public int compareTo(MostCommWritable_03 o) {
        if(this.hashCode() < o.hashCode())
            return -1;
        else if(this.hashCode() > o.hashCode())
            return +1;
        return 0;
    }

    @Override
    public int hashCode() {

        return Objects.hash(qtde, ano);
    }
    /*
     * tomar cuidado com a ordem em que os atributos são escritos e lidos
     *a ordem deve ser mantida
     * */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(qtde);
        dataOutput.writeUTF(ano);

    }

    @Override
    public String toString() {
        return
                " - " + ano +
                        " - quantidade = " + qtde;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        qtde = dataInput.readDouble();
        ano = dataInput.readUTF();
    }

}

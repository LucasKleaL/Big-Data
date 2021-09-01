package advanced.entropy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EntropyFASTA {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        Path intermediate = new Path("./output/intermediate.tmp");

        // arquivo de saida
        Path output = new Path(files[1]);

        // Job Etapa A
        Job j1 = new Job(c, "contagem");

        //  Registro de classes
        j1.setJarByClass(EntropyFASTA.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setReducerClass(ReduceEtapaA.class);

        //  Definição dos tipos de saida
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(LongWritable.class);

        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(LongWritable.class);

        // Cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        if(! j1.waitForCompletion(true)) {
            System.err.println("Erro no job1");
        }

        //  Job2
        Job j2 = new Job(c, "entropia");

        //  Registro de classes
        j2.setJarByClass(EntropyFASTA.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);

        //  Definição de tipos de saida
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(BaseQtdWritable.class);

        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(DoubleWritable.class);

        //  Cadastro de arquivos de entrada e saida
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        //  Executar o job 2
        if (! j2.waitForCompletion(true)) {
            System.err.println("Erro no job2");
        }


    }


    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //  Receber uma linha para processar
            String linha = value.toString();

            //  Ignorar o cabeçalho
            if(linha.startsWith(">")) return;

            //  Linha com conteúdo
            String[] caracteres = linha.split("");

            //  Laço de repetição para gerar as ocorrencias (contagem) (c, 1)
            for (String c : caracteres) {
                con.write(new Text(c), new LongWritable(1));
                con.write(new Text("total"), new LongWritable(1)); //contador global para os caracteres
            }

        }
    }

    public static class ReduceEtapaA extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {

            //  Soma as ocorrencias por chave
            long qtd = 0;
            for(LongWritable v : values) {
                qtd += v.get();
            }

            //  Resultado final: gerar (caracter, qtd)
            con.write(key, new LongWritable(qtd));

        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, BaseQtdWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //  Ler o arquivo intermediario linha a linha
            String linha = value.toString();

            //  Obtendo caracter e a quantidade usando um split por tab
            String[] campos = linha.split("\t");

            //  Caracter
            String c =campos[0];

            //  Qtd
            long qtd = Long.parseLong(campos[1]);

            //  Repassar essa informação para o reduce com chave unica
            con.write(new Text("entropia"), new BaseQtdWritable(c, qtd));

        }
    }

    public static class ReduceEtapaB extends Reducer<Text, BaseQtdWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<BaseQtdWritable> values, Context con)
                throws IOException, InterruptedException {

            //  Reduce vai receber ("entropia", [(A, 125), (C, ...)] )
            long total = 0;

            //  Buscando o valor total
            for (BaseQtdWritable o : values) {
                if (o.getCaracter().equals("total")) {
                    total = o.getQtd();
                    break;
                }
            }

            //  Percorrendo a lista novamente, calculando a probabilidade e entropia para os caracteres
            for(BaseQtdWritable o : values) {

                //  Se não for a chave total, calcula a entropia
                if(! o.getCaracter().equals("total")) {

                    double p = o.getQtd() / (double) total;
                    //  log2(X) = logY(X) / logY(2.0)
                    double entropia = -p * Math.log10(p) / Math.log10(2.0);

                    con.write(new Text(o.getCaracter()), new DoubleWritable(entropia));

                }
            }
        }
    }
}

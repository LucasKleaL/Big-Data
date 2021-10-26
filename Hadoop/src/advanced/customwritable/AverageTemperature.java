package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        //  arquivo de entrada
        Path input = new Path(files[0]);

        //  arquivo de saida
        Path output = new Path(files[1]);

        //  criacao do job e seu nome
        Job j = new Job(c, "media");

        //  Registro das classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        //  Definicao dos tipos de saida
        //  Map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);

        //  Reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        //  Cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //  lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //Obter o valor da linha
            String linha = value.toString();

            //Quebrando a linha em campos
            String campos[] = linha.split(",");

            //Acessar a posicao 8 (temperatura)
            double temperatura = Double.parseDouble(campos[8]);

            //Acessar a posicao 2 (mes)
            String mes = campos[2];

            //Ocorrencia
            int ocorrencia = 1;

            //Contexto: emitindo os dados do map para o sort/shuffle e em seguida para o reduce
            //Gerando resultado global
            con.write(new Text("unica"), new FireAvgTempWritable(ocorrencia, temperatura));

            //Gerando resultado para um mês especifico
            con.write(new Text(mes), new FireAvgTempWritable(ocorrencia, temperatura));

        }
    }

    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            //  Ideia geral do reduce:
            //  1 - Somar os Ns (ocorrencias)
            //  2 - Somar as temperaturas

            double somaTemps = 0.0;
            int somaNs = 0;

            for (FireAvgTempWritable o : values) {
                somaTemps += o.getTemp();
                somaNs += o.getN();
            }

            //  3 - Calcular a média

            double media = somaTemps / somaNs;

            //  4 - Joar a media do resultado no arquivo final
            con.write(new Text(key), new DoubleWritable(media));

        }
    }

}

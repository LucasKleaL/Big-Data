package basic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");

        //  Registro de classes
        j.setJarByClass(WordCount.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);

        //  Definição dos tipos de saida

        //map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        //reduce
        j.setOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        //  Cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //  Executar
        boolean b = j.waitForCompletion(true);

        if (b) {
            System.exit(0);
        }
        else {
            System.exit(1);
        }
    }

    /*
    *   A função map vai ser chamada por LINHA do arquivo
    *   Isso significa que eu não preciso fazer um laço de repetição para tratar multiplas linhas
    *
    *   Estrutura
    *
    *   Mapper <Tipo de chave de entrada, tipo do valor de entrada, tipo da chave de saída, tipo de valor de saída>
    */

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //extrair o conteudo da linha
            String conteudo = value.toString();

            //quebrando a linha em palvras
            String[] palavras = conteudo.split(" ");

            for (String p : palavras) {

                //gerando chave
                Text chaveSaida = new Text(p);

                //gerando valor
                IntWritable valorSaida = new IntWritable(1);

                //usando contexto para passar cahve e valor para o sort/shuffle
                con.write(chaveSaida, valorSaida);

            }
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            //key = minha palavra
            //value = lista de ocorrencias

            //somar para cada palavra(chave) a soma das ocorrencias

            int soma = 0;
            for (IntWritable v : values) {
                soma += v.get();
            }

            con.write(key, new IntWritable(soma));

        }
    }
}

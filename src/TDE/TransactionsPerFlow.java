package TDE;

import TDE.CustomWritable.TransactionsPerFlowWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TransactionsPerFlow {

    //  Esta classe é reponsável por contar quantas transações foram feitas por ano de acordo com seu tipo (importação/exportação)

    public static void main(String[] args) throws  Exception {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "TransactionsPerFlow");

        //  Registro de classes
        j.setJarByClass(TransactionsPerFlow.class);
        j.setMapperClass(TransactionsPerFlow.MapForTransactionsPerFlow.class);
        j.setReducerClass(TransactionsPerFlow.ReducerForTransactionsPerFlow.class);

        //  Definição dos tipos de saida

        //  Map
        j.setMapOutputKeyClass(TransactionsPerFlowWritable.class);
        j.setMapOutputValueClass(IntWritable.class);

        //  Reduce
        j.setOutputKeyClass(TransactionsPerFlowWritable.class);
        j.setOutputValueClass(IntWritable.class);

        //  Cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        if(! j.waitForCompletion(true)) {
            System.err.println("Erro no job");
        }

    }

    public static class MapForTransactionsPerFlow extends Mapper<Object, Text, TransactionsPerFlowWritable, IntWritable> {

        public void map(Object key, Text value, Context con)
                throws IOException, InterruptedException {

            //  Extraindo o conteudo da linha
            String content = value.toString();

            if (content.startsWith("country_or_area")) {
                return;
            }

            //  Quebrando a linha
            String[] contentSplit = content.split(";");

            //  Acessar o ano (posição 1)
            String year = contentSplit[1];

            //  Acessar o flow (posicao 4)
            String flowType = contentSplit[4];

            int occurrence = 1;

            con.write(new TransactionsPerFlowWritable(year, flowType), new IntWritable(occurrence));

        }

    }

    public static class ReducerForTransactionsPerFlow extends Reducer<TransactionsPerFlowWritable, IntWritable, TransactionsPerFlowWritable, IntWritable> {

        public void reduce(TransactionsPerFlowWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable v : values) {
                sum += v.get();
            }

            con.write(key, new IntWritable(sum));

        }

    }

}

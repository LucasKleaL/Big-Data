package TDE;

import java.io.IOException;

import basic.WordCount;
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

public class BrazilCount {

    public static void main(String[] args) throws  Exception {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "brazilcount");

        //  Registro de classes
        j.setJarByClass(BrazilCount.class);
        j.setMapperClass(MapForBrazilCount.class);
        j.setReducerClass(ReducerForBrazilCount.class);

        //  Definição dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        //  Cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        if(! j.waitForCompletion(true)) {
            System.err.println("Erro no job");
        }

    }

    public static class MapForBrazilCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
            throws  IOException, InterruptedException {

            //  Extraindo o conteudo da linha
            String content = value.toString();

            //  Quebrando a linha
            String[] contentSplit = content.split(";");

            //  Acessar o país (posição 0)
            String country = contentSplit[0];

            int ocorrency = 1;

            con.write(new Text(country), new IntWritable(ocorrency));

            /*
            for (String p : contentSplit) {

                //  Gerando chave
                Text outKey = new Text(p);

                //  Gerando valor
                IntWritable outValue = new IntWritable(1);

                con.write(outKey, outValue);

            }
             */

        }

    }

    public static class ReducerForBrazilCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
            throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }

            con.write(key, new IntWritable(sum));

        }

    }

}

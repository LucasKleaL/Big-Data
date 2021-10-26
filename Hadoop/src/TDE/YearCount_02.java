package TDE;

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

import java.io.IOException;

public class YearCount_02 {

    //  Esta classe é reponsável por contar quantas transações foram feitas por ano

    public static void main(String[] args) throws  Exception {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "yearcount");

        //  Registro de classes
        j.setJarByClass(YearCount_02.class);
        j.setMapperClass(YearCount_02.MapForYearCount.class);
        j.setReducerClass(YearCount_02.ReducerForYearCount.class);

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

    public static class MapForYearCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //  Extraindo o conteudo da linha
            String content = value.toString();

            //  Quebrando a linha
            String[] contentSplit = content.split(";");

            //  Acessar o ano (posição 1)
            String year = contentSplit[1];

            int occurrence = 1;

            con.write(new Text(year), new IntWritable(occurrence));

        }

    }

    public static class ReducerForYearCount extends Reducer<Text, IntWritable, Text, IntWritable> {

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

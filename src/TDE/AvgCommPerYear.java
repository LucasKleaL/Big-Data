package TDE;

import TDE.CustomWritable.AvgCommPerYearWritable;
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

public class AvgCommPerYear {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        //  Arquivo de entrada
        Path input = new Path(files[0]);

        //  Arquivo de saida
        Path output = new Path(files[1]);

        //  Criacao do job
        Job j = new Job(c, "TransactionsAvg");

        //  Registro das classes
        j.setJarByClass(AvgCommPerYear.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        //  Map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(AvgCommPerYearWritable.class);

        //  Reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
        
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, AvgCommPerYearWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("country_or_area")) return;

            String content[] = line.split(";");

            String year = content[1];

            double price = Double.parseDouble(content[5]);

            con.write(new Text(year), new AvgCommPerYearWritable(1, price));

        }

    }

    public static class ReduceForAverage extends Reducer<Text, AvgCommPerYearWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<AvgCommPerYearWritable> values, Context con)
                throws IOException, InterruptedException {

            double sum = 0.0;
            int sumCount = 0;
            for (AvgCommPerYearWritable o : values) {
                sum += o.getCommValue();
                sumCount += o.getCount();
            }

            double avg = sum / sumCount;

            con.write(new Text(key), new DoubleWritable(avg));

        }

    }

}

package TDE;

import TDE.CustomWritable.HighestPrice2Writable;
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

public class HighestPrice2 {

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
        j.setJarByClass(HighestPrice2.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        //  Map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(HighestPrice2Writable.class);

        //  Reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);

    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, HighestPrice2Writable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Pega linha
            String linha = value.toString();

            if (linha.startsWith("country_or_area")) return;

            // Quebra em campos
            String[] campos = linha.split(",");

            // Pega ano, mercadoria e preco
            String year = campos[1];
            double price = Double.parseDouble(campos[5]);
            double quantity = Double.parseDouble(campos[8]);

            // enviando informacao pro reduce
            con.write(new Text(year), new HighestPrice2Writable(quantity, price));

        }

    }

    public static class ReduceForAverage extends Reducer<Text, HighestPrice2Writable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<HighestPrice2Writable> values, Context con)
                throws IOException, InterruptedException {

            double maxPricePorUnity = Double.MIN_VALUE;

            for(HighestPrice2Writable o : values){
                if ((o.getPrice() / o.getUnit()) > maxPricePorUnity)
                    maxPricePorUnity = (o.getPrice() / o.getUnit());
            }

            // escrevendo os maiores valores em arquivo
            con.write(new Text(key), new DoubleWritable(maxPricePorUnity));
        }

    }

}

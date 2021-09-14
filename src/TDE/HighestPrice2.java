package TDE;

import TDE.CustomWritable.HighestPrice2Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class HighestPrice2 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "Seis");

        // registro de classes
        j.setJarByClass(HighestPrice2.class);
        j.setMapperClass(SeisMapper.class);
        j.setReducerClass(SeisReducer.class);

        // definicao dos tipos de saida
        // map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(HighestPrice2Writable.class);

        // reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // definicao dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // executa o job
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class SeisMapper extends Mapper<Object, Text, Text, HighestPrice2Writable> {
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

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
            context.write(new Text(year), new HighestPrice2Writable(quantity, price));
        }
    }

    public static class SeisReducer extends Reducer<Text, HighestPrice2Writable, Text, DoubleWritable> {

        public void reduce(Text key,
                           Iterable<HighestPrice2Writable> values,
                           Context context) throws IOException, InterruptedException {
            // define variaveis para maior preco e mercadoria para um ano que chega como chave

            double maxPricePorUnity = Double.MIN_VALUE;

            for(HighestPrice2Writable o : values){
                if ((o.getPrice() / o.getUnit()) > maxPricePorUnity)
                    maxPricePorUnity = (o.getPrice() / o.getUnit());
            }

            // escrevendo os maiores valores em arquivo
            context.write(key, new DoubleWritable(maxPricePorUnity));

        }
    }

}
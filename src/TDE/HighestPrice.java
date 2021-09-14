package TDE;

import java.io.IOException;

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

public class HighestPrice {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        //  arquivo de entrada
        Path input = new Path(files[0]);

        //  arquivo de saida
        Path output = new Path(files[1]);

        //  criacao do job e seu nome
        Job j = new Job(c, "highestPrice");

        //  registro de classes
        j.setJarByClass(HighestPrice.class);
        j.setMapperClass(HighestPrice.HighestPriceMapper.class);
        j.setReducerClass(HighestPrice.HighestPriceReducer.class);

        //  definição dos tipos de saida
        //  map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DoubleWritable.class);

        //  reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        //  definição dos tipos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public class HighestPriceMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context con) throws IOException,
                InterruptedException {

            //  Obtendo o valor da linha
            String content = value.toString();

            if (content.startsWith("country_or_area")) {
                return;
            }

            //  Quebrando a linha em campos
            String contentSplit[] = content.split(";");

            String keyValue = contentSplit[3] + " " + contentSplit[1];

            double price = Double.parseDouble(contentSplit[5]);

            con.write(new Text(keyValue), new DoubleWritable(price));

            /*
            //  Acessando a posição 1 (ano)
            String year = contentSplit[1];

            //  Acessando a posição 3 (commodity)
            String commodity = contentSplit[3];

            //  Acessando a posição 5 (preço)
            double price = Double.parseDouble(contentSplit[5]);

            //  Emitindo os parametros do contexto
            con.write(new Text(year), new HighestPriceWritable(commodity, price));
             */

        }
    }

    public class HighestPriceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key,
                           Iterable<DoubleWritable> values,
                           Context con) throws IOException, InterruptedException {

            String commodity = "";
            double maxPrice = Double.MIN_VALUE;

            for (DoubleWritable v : values) {
                if (v.get() > maxPrice) {
                    maxPrice = v.get();
                }
            }

            con.write(key, new DoubleWritable(maxPrice));

        }
    }

}

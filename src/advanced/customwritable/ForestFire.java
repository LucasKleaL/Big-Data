package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

public class ForestFire {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "forestfire");

        //  registro de classes
        j.setJarByClass(ForestFire.class);
        j.setMapperClass(ForestFireMapper.class);
        j.setReducerClass(ForestFireReducer.class);

        //  definição dos tipos de saida
        // map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(ForestFireWritable.class);

        //  reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(ForestFireWritable.class);

        //  definição dos tipos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public class ForestFireMapper extends Mapper<Object, Text, Text, ForestFireWritable> {
        public void map(Object key, Text value, Context con) throws IOException,
                InterruptedException {

            //  Obtendo o valor da linha
            String line = value.toString();

            //  Quebrando a linha em campos
            String campos[] = line.split(",");

            //  Acessando a posição 2 (mes)
            String month = campos[2];

            //  Acessando a posição 8 (temperatura)
            double temp = Double.parseDouble(campos[8]);

            //  Acessando a posição 10 (vento)
            double wind = Double.parseDouble(campos[10]);

            //  Emitindo os parametros do contexto
            con.write(new Text(month), new ForestFireWritable(temp, wind));

        }
    }

    public class ForestFireReducer extends Reducer<Text, ForestFireWritable, Text, ForestFireWritable> {

        public void reduce(Text key,
                           Iterable<ForestFireWritable> values,
                           Context con) throws IOException, InterruptedException {

            double maxTemp = Double.MIN_VALUE;
            double maxWind = Double.MIN_VALUE;

            for (ForestFireWritable o : values) {
                if (o.getTemp() > maxTemp) {
                    maxTemp = o.getTemp();
                }
                if (o.getWind() > maxWind) {
                    maxWind = o.getWind();
                }
            }

            con.write(key, new ForestFireWritable(maxTemp, maxWind));

        }
    }

}

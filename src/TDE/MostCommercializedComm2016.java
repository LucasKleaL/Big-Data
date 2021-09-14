package TDE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class MostCommercializedComm2016 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // Job Etapa A
        Job j1 = new Job(c, "contagem");

        //  Registro de classes
        j1.setJarByClass(MostCommercializedComm2016.class);
        j1.setMapperClass(MostCommercializedComm2016.MostCommercializedComm2016Map.class);
        j1.setReducerClass(MostCommercializedComm2016.MostCommercializedComm2016Reduce.class);

        //  Definição dos tipos de saida
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(DoubleWritable.class);

        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(DoubleWritable.class);

        // Cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, output);

        if(! j1.waitForCompletion(true)) {
            System.err.println("Erro no job");
        }

    }

    public static class MostCommercializedComm2016Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //  Receber uma linha para processar
            String line = value.toString();

            //  Ignorar o cabeçalho
            if(line.startsWith("country_or_area")) return;

            //  Linha com conteúdo
            String[] content = line.split(";");

            String year = content[1];
            String flow = content[4];

            double quantity = Double.MIN_VALUE;

            if (!content[8].equals("")) {
                quantity = Double.parseDouble(content[8]);
            }

            int occurrence = 1;

            if (year.equals("2016")) {
                con.write(new Text(flow), new DoubleWritable(quantity));
            }

        }
    }

    public static class MostCommercializedComm2016Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            //  Soma as ocorrencias por chave
            long sum = 0;
            for(DoubleWritable v : values) {
                sum += v.get();
            }

            //  Resultado final: gerar (caracter, qtd)
            con.write(key, new DoubleWritable(sum));

        }
    }

}

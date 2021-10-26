package TDE;

import TDE.CustomWritable.MostCommWritable_03;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class MostCommercializedCommodity_03 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        //  Criacao do job
        Job job = new Job(c, "CommoditiesPorFlow");

        //  Registro de classes
        job.setJarByClass(MostCommercializedCommodity_03.class);
        job.setMapperClass(MapForSumm.class);
        job.setReducerClass(ReduceForSumm.class);

        //  Map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MostCommWritable_03.class);
        // Reduce
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MostCommWritable_03.class);

        // Cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class MapForSumm extends Mapper<LongWritable, Text, Text, MostCommWritable_03> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();

            //  Ignora o cabeçalho
            if(line.startsWith("country_or_area")) return;

            //  Linha com conteúdo
            String[] content = line.split(";");

            String ano = content[1];
            String flow = content[4];

            double qtde = Double.MIN_VALUE;

            if (!content[8].equals("")) {
                qtde = Double.parseDouble(content[8]);
            }

            if (ano.equals("2016")) {
                con.write(new Text(flow), new MostCommWritable_03(qtde, ano));
            }

        }
    }

    public static class ReduceForSumm extends Reducer<Text, MostCommWritable_03, Text, MostCommWritable_03> {
        public void reduce(Text key, Iterable<MostCommWritable_03> values, Context con)
                throws IOException, InterruptedException {
            double qtde = 0;
            String ano = "";
            for(MostCommWritable_03 o : values) {
                qtde += o.getQtde();
                ano = o.getAno();
            }
            con.write(key, new MostCommWritable_03(qtde, ano));

        }
    }

}

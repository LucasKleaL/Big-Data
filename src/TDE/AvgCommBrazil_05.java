package TDE;

import TDE.CustomWritable.AvgCommBrazilWritable_05;
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

public class AvgCommBrazil_05 {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "AvgCommBrazil");

        j.setJarByClass(AvgCommBrazil_05.class);
        j.setMapperClass(AvgCommBrazilMapper.class);
        j.setReducerClass(AvgCommBrazilReducer.class);

        //  Map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(AvgCommBrazilWritable_05.class);

        //  Reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);

    }

    public static class AvgCommBrazilMapper extends Mapper<LongWritable, Text, Text, AvgCommBrazilWritable_05> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line.startsWith("country_or_area")) return;
            String[] data = line.split(";");

            String country = data[0];
            String year = data[1];
            String flow = data[4];
            double price = Double.parseDouble(data[5]);
            String unitType = data[7];
            String category = data[9];

            String content = unitType + " " + category + " " +  year;

            int occurrence = 1;

            if (country.equals("Brazil") && (flow.equals("Export"))) {
                con.write(new Text(content), new AvgCommBrazilWritable_05(price, occurrence));

            }

        }

    }

    public static class AvgCommBrazilReducer extends Reducer<Text, AvgCommBrazilWritable_05, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<AvgCommBrazilWritable_05> values, Context con)
                throws IOException, InterruptedException {

            double sumPrice = 0.0;
            int sumCounter = 0;
            for (AvgCommBrazilWritable_05 v : values) {
                sumPrice += v.getValue();
                sumCounter += v.getCounter();
            }
            double avg = sumPrice / sumCounter;
            con.write(key, new DoubleWritable(avg));

        }

    }

}

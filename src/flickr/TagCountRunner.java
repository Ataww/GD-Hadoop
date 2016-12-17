package flickr;

import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Ataww on 08/12/2016.
 */
public class TagCountRunner {

    public static class TagCountryMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FlickrEntry entry = new FlickrEntry(value.toString());
            for (String s : entry.getTags()) {
                if(entry.getCountry() == null) {
                    break;
                }
                context.write(new Text(entry.getCountry()), new StringAndInt(s,1));
            }
        }
    }

    public static class TagCountryCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {

        @Override
        protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
            Map<String, AtomicInteger> tagCounts = new HashMap<>();
            for(StringAndInt t : values) {
                if (!tagCounts.containsKey(t.getTag().toString())) {
                    tagCounts.put(t.getTag().toString(), new AtomicInteger(1));
                } else {
                    tagCounts.get(t.getTag().toString()).incrementAndGet();
                }
            }
            for(Map.Entry<String, AtomicInteger> e : tagCounts.entrySet()) {
                context.write(key, new StringAndInt(e.getKey(), e.getValue().get()));
            }
        }
    }

    public static class TagCountryReducer extends Reducer<Text, StringAndInt, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
            Map<String, AtomicInteger> tagCounts = new HashMap<>();
            for (StringAndInt t : values) {
                if (!tagCounts.containsKey(t.getTag().toString())) {
                    tagCounts.put(t.getTag().toString(), new AtomicInteger(t.getCount()));
                } else {
                    tagCounts.get(t.getTag().toString()).addAndGet(t.getCount());
                }
            }
            MinMaxPriorityQueue<StringAndInt> populars = MinMaxPriorityQueue.maximumSize(context.getConfiguration().getInt("K", 1)).create();
            for(Map.Entry<String, AtomicInteger> e : tagCounts.entrySet()) {
                StringAndInt couple = new StringAndInt(e.getKey(),e.getValue().get());
                populars.add(couple);
            }
            StringBuilder sb = new StringBuilder();
            while(!populars.isEmpty()) {
                StringAndInt pop = populars.poll();
                context.write(new Text("("+key.toString()+","+pop.getTag()+")"), new IntWritable(pop.getCount()));
                sb.append(pop.getTag()+"["+pop.getCount()+"],");
            }
            sb.deleteCharAt(sb.length() - 1);
            //context.write(key,new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("K", Integer.parseInt(args[2]));
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf, "tag country");
        job.setJarByClass(TagCountRunner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StringAndInt.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TagCountryMapper.class);
        job.setCombinerClass(TagCountryCombiner.class);
        job.setReducerClass(TagCountryReducer.class);
        job.setNumReduceTasks(3);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

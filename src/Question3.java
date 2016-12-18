import com.google.common.collect.MinMaxPriorityQueue;
import flickr.CountryAndTag;
import flickr.FlickrEntry;
import flickr.StringAndInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Ataww on 08/12/2016.
 */
public class Question3 {

    public static class TagCountryMapper extends Mapper<LongWritable, Text, CountryAndTag, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FlickrEntry entry = new FlickrEntry(value.toString());
            for (String s : entry.getTags()) {
                if(entry.getCountry() == null) {
                    break;
                }
                context.write(new CountryAndTag(entry.getCountry(),s), new IntWritable(1));
            }
        }
    }

    public static class TagCountryCombiner extends Reducer<CountryAndTag, IntWritable, CountryAndTag, IntWritable> {

        @Override
        protected void reduce(CountryAndTag key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Map<String, AtomicInteger> tagCounts = new HashMap<>();
            for (IntWritable t : values) {
                if (!tagCounts.containsKey(key.getTag())) {
                    tagCounts.put(key.getTag(), new AtomicInteger(1));
                } else {
                    tagCounts.get(key.getTag()).incrementAndGet();
                }
            }
            for(Map.Entry<String, AtomicInteger> e : tagCounts.entrySet()) {
                context.write(new CountryAndTag(key.getCountry(),e.getKey()), new IntWritable(e.getValue().intValue()));
            }
        }
    }

    public static class TagCountryReducer extends Reducer<CountryAndTag, IntWritable, CountryAndTag, IntWritable> {
        @Override
        protected void reduce(CountryAndTag key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Map<String, AtomicInteger> tagCounts = new HashMap<>();
            for (IntWritable t : values) {
                if (!tagCounts.containsKey(key.getTag())) {
                    tagCounts.put(key.getTag(), new AtomicInteger(1));
                } else {
                    tagCounts.get(key.getTag()).incrementAndGet();
                }
            }
            for(Map.Entry<String, AtomicInteger> e : tagCounts.entrySet()) {
                context.write(new CountryAndTag(key.getCountry(),e.getKey()), new IntWritable(e.getValue().intValue()));
            }
        }
    }

    public static class TagCountryMapper_2 extends Mapper<CountryAndTag, IntWritable, Text, StringAndInt> {
        @Override
        protected void map(CountryAndTag key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.getCountry()), new StringAndInt(key.getTag(),value.get()));
        }
    }

    public static class TagCountryReducer_2 extends Reducer<Text, StringAndInt, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
            MinMaxPriorityQueue<StringAndInt> populars = MinMaxPriorityQueue.maximumSize(context.getConfiguration().getInt("K", 1)).create();

            for(StringAndInt e : values) {
                StringAndInt tmp = new StringAndInt(e.getTag(),e.getCount());
                populars.add(tmp);
            }

            StringBuilder sb = new StringBuilder();

            while(!populars.isEmpty()) {
                StringAndInt pop = populars.poll();
                context.write(new Text("("+key.toString()+","+pop.getTag()+")"), new IntWritable(pop.getCount()));
                sb.append(pop.getTag()+"["+pop.getCount()+"],");
            }
            sb.deleteCharAt(sb.length() - 1);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("K", Integer.parseInt(args[3]));
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job1 = Job.getInstance(conf, "tag country");
        job1.setJarByClass(Question3.class);
        job1.setMapOutputKeyClass(CountryAndTag.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(CountryAndTag.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setMapperClass(TagCountryMapper.class);
        job1.setCombinerClass(TagCountryCombiner.class);
        job1.setReducerClass(TagCountryReducer.class);
        job1.setNumReduceTasks(3);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "tag country");
        job2.setJarByClass(Question3.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(StringAndInt.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setMapperClass(TagCountryMapper_2.class);
        job2.setReducerClass(TagCountryReducer_2.class);
        job2.setNumReduceTasks(3);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

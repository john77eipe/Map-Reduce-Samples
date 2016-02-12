package com.sample.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
Rewrite the word count example by utilizing Tool runner and 
writing the output to 26 different files corresponding to 26 alphabets (based on the word's starting character)
 *
 */
public class WordCountMultiple extends Configured implements Tool{
	
	public static class MapperStub extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word;

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			word = new Text();
		}
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = Character.isAlphabetic(itr.nextToken().toLowerCase().charAt(0))?itr.nextToken():null;
				if(token!=null) {
					word.set(token);
					context.write(word, one);
				}
				
			}
		}
	}

	public static class ReducerStub extends Reducer<Text, IntWritable, Text, IntWritable> {

		private MultipleOutputs<Text, IntWritable> multipleResults;

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			multipleResults =  new MultipleOutputs<Text, IntWritable>(context);
		}
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			multipleResults.write(key, new IntWritable(sum), key.toString().substring(0,1));
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println("*******Word-Count-Job starts********");
		System.exit(ToolRunner.run(new Configuration(), new WordCountMultiple(), args));
		System.out.println("********Word-Count-Job ends********");
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Word-Count-Job");
		job.setJarByClass(WordCountMultiple.class);
		job.setMapperClass(MapperStub.class);
		job.setCombinerClass(ReducerStub.class);
		job.setReducerClass(ReducerStub.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}

package com.sample.lettercount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LetterCount {

	public static class MapperStub extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				char[] characters = itr.nextToken().toCharArray();
				for (int i = 0; i < characters.length; i++) {
					word.set(Character.toString(characters[i]));
					context.write(word, one);
				}
			}
		}
	}

	public static class ReducerStub extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		System.out.println("*******Letter-Count-Job starts********");

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Letter-Count-Job");
		job.setJarByClass(LetterCount.class);
		job.setMapperClass(MapperStub.class);
		job.setCombinerClass(ReducerStub.class);
		job.setReducerClass(ReducerStub.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println("********Letter-Count-Job execution starts********");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("********Letter-Count-Job terminated********");
	}

}

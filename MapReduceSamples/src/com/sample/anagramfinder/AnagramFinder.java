package com.sample.anagramfinder;

import java.io.IOException;
import java.util.Arrays;
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

/**
Find anagrams in a huge text. An anagram is basically a
different arrangement of letters in a word. Anagram does not
need have a meaning.
Input:
“the cat act in tic tac toe.”
Output:
cat, tac, act
the
toe
in
tic
 *
 */
public class AnagramFinder {

	public static class MapperStub extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				char[] charArr = itr.nextToken().toCharArray();
				char[] originalArr = charArr.clone();
				Arrays.sort(charArr);
				context.write(new Text(charArr.toString()), new Text(originalArr.toString()));
			}

		}
	}

	public static class ReducerStub extends Reducer<Text, Text, Text, Iterable<Text>> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
			context.write(key, values);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Anagram-Finder-Job");
		job.setJarByClass(AnagramFinder.class);
		job.setMapperClass(MapperStub.class);
		job.setCombinerClass(ReducerStub.class);
		job.setReducerClass(ReducerStub.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

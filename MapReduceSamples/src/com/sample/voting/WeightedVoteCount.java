package com.sample.voting;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sample.dnaseq.DNAGrouping;
import com.sample.dnaseq.DNAGrouping.MapperStub;
import com.sample.dnaseq.DNAGrouping.ReducerStub;
import com.sample.wordcount.WordCount;

/**
In an unusual democracy, everyone is not equal. The vote count is a
function of worth of the voter. Though everyone is voting for each other.
As example, if A with a worth of 5 and B with a worth of 1 are voting
for C, the vote count of C would be 6.
You are given a list of people with their value of vote. You are also given
another list describing who voted for who all.
List1
Voter Votee
A C
B C
C F
Find out what is the vote count of everyone?
List2
PersonWorth
A 5
B 1
C 11
Result
PersonVoteCount
A 0
B 0
C 6
F 11
 *
 */
public class WeightedVoteCount  extends Configured implements Tool {
	
	public static class MapperStub extends Mapper<Object, Text, Text, Text> {
		
		private Text outputKey;
		private Text outputValue;
		
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			outputKey = new Text();
			outputValue = new Text();
		}
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				String[] input = itr.nextToken().split(" ");
				outputKey.set(input[1]);
				outputValue.set(input[0]);
				context.write(outputKey, outputValue);
			}
		}
	}
	
	public static class ReducerStub extends Reducer<Text, Text, NullWritable, Text> {
		
		private NullWritable outputKey;
		private Text outputValue;
		
		@Override
		protected void setup(Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			outputKey = null;
			outputValue = new Text();
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder output = new StringBuilder();
			Iterator<Text> i = values.iterator();
			while(true){
				output.append(i.next());
				if(i.hasNext()){
					output.append(", ");
				}else {
					break;
				}
			}
			outputValue.set(output.toString());
			context.write(outputKey, outputValue);
		}
	}
	

	public static void main(String[] args) throws Exception {
		System.out.println("*******WeightedVoteCount-Job starts********");
		System.exit(ToolRunner.run(new Configuration(), new WeightedVoteCount(), args));
		System.out.println("********WeightedVoteCount-Job ends********");
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "WeightedVoteCount-Job");
		job.setJarByClass(WordCount.class);
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

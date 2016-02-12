package com.sample.dnaseq;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sample.wordcount.WordCount;

/**
 A file contains the DNA sequence of people. Find all the
people who have same DNAs.
Input:
“User1 ACGT”
“User2 TGCA”
“User3 ACG”
“User4 ACGT”
“User5 ACG”
“User6 AGCT”
Output:
User1, User4
User2
User3, User 5
User6

 *
 */
public class DNAGrouping extends Configured implements Tool{
	
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
		System.out.println("*******DNA-Grouping-Job starts********");
		System.exit(ToolRunner.run(new Configuration(), new DNAGrouping(), args));
		System.out.println("********DNA-Grouping-Job ends********");
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "DNA-Grouping-Job");
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

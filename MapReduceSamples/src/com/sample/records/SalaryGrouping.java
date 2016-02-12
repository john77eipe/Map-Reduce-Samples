package com.sample.records;

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
 * Custom value class - needs to implement Writable interface
 *
 */
public class SalaryGrouping extends Configured implements Tool{
	
	public static class MapperStub extends Mapper<Object, Text, IntWritable, UserRecord> {

		private static IntWritable salary;
		private static UserRecord user;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			salary = new IntWritable();
			
		}
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				String[] values = token.split(" ");
				
				salary.set(Integer.parseInt(values[2]));
				user = new UserRecord(values[0], values[1], Integer.parseInt(values[3]));
				context.write(salary, user );
			
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
		System.out.println("*******SalaryGrouping-Job starts********");
		System.exit(ToolRunner.run(new Configuration(), new SalaryGrouping(), args));
		System.out.println("********SalaryGrouping-Job ends********");
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "SalaryGrouping-Job");
		job.setJarByClass(SalaryGrouping.class);
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

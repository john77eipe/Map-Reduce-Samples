package com.sample.custom.outputformat;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

public class CustomFileOutputFormat <K, V> extends FileOutputFormat {

	  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
	      String name, Progressable progress) throws IOException {
	    Path file = FileOutputFormat.getTaskOutputPath(job, name);
	    FileSystem fs = file.getFileSystem(job);
	    FSDataOutputStream fileOut = fs.create(file, progress);
	    return new CustomRecordWriter<K, V>(fileOut);
	  }
	}
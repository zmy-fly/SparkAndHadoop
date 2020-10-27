package com.atguigu.mr.weblog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-25-10:04
 */
public class LogDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(LogDriver.class);
//		job.set
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		Path inputPath = new Path("D:\\HdfsClientDemo\\input\\web.log");
		Path outputPath = new Path("D:\\HdfsClientDemo\\output");
		FileSystem fs = FileSystem.get(configuration);
		if(fs.exists(outputPath)){
			fs.delete(outputPath,true);
		}
		job.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(job,inputPath);
		FileOutputFormat.setOutputPath(job,outputPath);
		job.waitForCompletion(true);
	}
}

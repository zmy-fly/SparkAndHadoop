package com.school.mapreduce.ratecount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-22-9:47
 */

public class SortDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		job.setJarByClass(SortDriver.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);

		job.setMapOutputKeyClass(SortBean.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(SortBean.class);

		Path path = new Path("D:\\HdfsClientDemo\\output");
		FileSystem fs = FileSystem.get(configuration);
		if(fs.exists(path)){
			fs.delete(path,true);
		}
		FileInputFormat.setInputPaths(job, new Path("TextSortBean.txt"));
		FileOutputFormat.setOutputPath(job,path);
		job.waitForCompletion(true);

	}
}

package com.school.mapreduce.topN1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-10-09-11:51
 */
public class TopNDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		job.setJarByClass(TopNDriver.class);

		job.setMapperClass(TopNMapper.class);

//		job.setReducerClass(TopNReducer.class);

//		job.setMapOutputKeyClass(CourseScore.class);
//		job.setMapOutputKeyClass(NullWritable.class);

		job.setOutputKeyClass(CourseScore.class);
		job.setOutputValueClass(NullWritable.class);

		job.setPartitionerClass(TopNPartitioner.class);
		job.setNumReduceTasks(4);




		Path path = new Path("D:\\HdfsClientDemo\\output");

		FileSystem fs = FileSystem.get(configuration);
		if(fs.exists(path)){
			fs.delete(path,true);
		}

		FileInputFormat.setInputPaths(job,"D:\\HdfsClientDemo\\input\\score.txt");
		FileOutputFormat.setOutputPath(job,path);
		job.waitForCompletion(true);

	}
}

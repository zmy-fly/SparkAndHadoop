package com.school.mapreduce.table;

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
 * @create 2020-09-24-10:01
 */
public class TableDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		args = new String[]{"D:\\HdfsClientDemo\\input"};
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(TableDriver.class);

		job.setMapperClass(TableMapper.class);
		job.setReducerClass(TableReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TableBean.class);

		job.setOutputKeyClass(TableBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileSystem fs = FileSystem.get(configuration);
		Path path = new Path("D:\\HdfsClientDemo\\output");
		if(fs.exists(path)){
			fs.delete(path,true);
		}
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,path);
		job.waitForCompletion(true);
	}
}

package com.atguigu.mr.nline;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

/**
 * @author Bonze
 * @create 2020-09-20-23:12
 */
public class NLineDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		NLineInputFormat.setNumLinesPerSplit(job, 3);
		job.setInputFormatClass(NLineInputFormat.class);
		job.setJarByClass(NLineDriver.class);
		job.setMapperClass(NLineMapper.class);
		job.setReducerClass(NLineReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		Path path = new Path("D:\\HdfsClientDemo\\output");
		File file = new File(String.valueOf(path));
		if (file.exists()) {
			FileUtils.deleteDirectory(file);
		}
		FileInputFormat.setInputPaths(job, "TextNLineInputFormat.txt");
		FileOutputFormat.setOutputPath(job, path);
		job.waitForCompletion(true);

	}
}





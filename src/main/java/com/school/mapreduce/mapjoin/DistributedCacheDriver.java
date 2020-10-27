package com.school.mapreduce.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Bonze
 * @create 2020-09-25-8:52
 */
public class DistributedCacheDriver {
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		job.setJarByClass(DistributedCacheDriver.class);
		job.setMapperClass(DistributedCacheMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		Path inputPath = new Path("D:\\HdfsClientDemo\\input");
		Path outputPath = new Path("D:\\HdfsClientDemo\\output");
		FileSystem fs = FileSystem.get(configuration);
		if(fs.exists(outputPath)){
			fs.delete(outputPath,true);
		}

		FileInputFormat.setInputPaths(job,inputPath);
		FileOutputFormat.setOutputPath(job,outputPath);
		job.addCacheFile(new URI("file:///D:/HdfsClientDemo/input/pd.txt"));
		job.setNumReduceTasks(0);
		boolean b = job.waitForCompletion(true);
		System.exit(b? 0: 1);

	}
}

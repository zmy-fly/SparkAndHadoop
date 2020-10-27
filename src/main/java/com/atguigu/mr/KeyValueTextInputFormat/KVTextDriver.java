package com.atguigu.mr.KeyValueTextInputFormat;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-20-22:02
 */
public class KVTextDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[]{"Text.txt", ""};
		Configuration configuration = new Configuration();
		configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
		Job job = Job.getInstance(configuration);
		job.setJarByClass(KVTextDriver.class);
		job.setMapperClass(KVTextMapper.class);
		job.setReducerClass(KVTextReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		Path path = new Path("D:\\HdfsClientDemo\\output");
		File file = new File(String.valueOf(path));
		if(file.exists()){
			FileUtils.deleteDirectory(file);
		}

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(job,path);
		job.waitForCompletion(true);


	}
}

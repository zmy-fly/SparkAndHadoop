package com.school.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-15-10:52
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

//        configuration.set("fs.defaultFS","hdfs://master2:9000");

        Job job = Job.getInstance();
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer01.class);
        job.setPartitionerClass(WordCountPartitioner01.class);
        job.setNumReduceTasks(3);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileSystem fs = FileSystem.get(configuration);
        Path path = new Path("D:\\output");

        if(fs.exists(path)){
            fs.delete(path, true);
        }

        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\Bonze\\Desktop\\today.txt"));
        FileOutputFormat.setOutputPath(job,path);

        boolean isDone = job.waitForCompletion(true);
        System.exit(isDone ? 0 : 1);

    }
}

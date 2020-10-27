package com.atguigu.mr.flowsum;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-20-17:28
 */
public class FlowsumDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"phone_data  (2).txt","D:\\HdfsClientDemo\\output"};
        Configuration configuration = new Configuration();
        // 获取job对象
        Job job = Job.getInstance(configuration);
        // 设置jar的路径
        job.setJarByClass(FlowsumDriver.class);
        // 关联mapper和reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);
        // 设置mapper输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        // 设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 设置输入输出路径
        Path path = new Path("D:\\HdfsClientDemo\\output");
        File file = new File(String.valueOf(path));
        if(file.isDirectory()){
            FileUtils.deleteDirectory(file);
        }
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        // 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0 : 1);


    }
}

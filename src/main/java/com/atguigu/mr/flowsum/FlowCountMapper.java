package com.atguigu.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-18-20:58
 */
public class FlowCountMapper extends Mapper<LongWritable, Text,Text, FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 先获取一行
        String line = value.toString();
        // 切割一下
        String[] split = line.split("\t");
        // 封装对象
        k.set(split[1]);
        long upFlow = Long.parseLong(split[split.length-3]);
        long downFlow = Long.parseLong(split[split.length-3]);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);
        context.write(k,v);
    }
}

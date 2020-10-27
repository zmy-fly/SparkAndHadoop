package com.atguigu.mr.KeyValueTextInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-20-21:27
 */
public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable> {
	LongWritable v = new LongWritable(1);
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
//		String s = key.toString();
//		String[] s1 = s.split(" ");
//		String str = s1[0];
//		key.set(str);
		context.write(key,v);
	}

}

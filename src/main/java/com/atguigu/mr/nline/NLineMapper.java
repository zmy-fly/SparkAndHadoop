package com.atguigu.mr.nline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-20-22:49
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	LongWritable longWritable = new LongWritable(1);
	Text text = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split(" ");
		for(int i = 0; i < split.length; i++){
			text.set(split[i]);
			context.write(text,longWritable);
		}
	}
}

package com.atguigu.mr.nline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-20-23:08
 */
public class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	LongWritable longWritable = new LongWritable(0);
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (LongWritable value : values) {
			sum += value.get();
		}
		longWritable.set(sum);
		context.write(key,longWritable);
	}
}

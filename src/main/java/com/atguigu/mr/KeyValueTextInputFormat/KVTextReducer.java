package com.atguigu.mr.KeyValueTextInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-20-21:43
 */
public class KVTextReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	LongWritable v = new LongWritable(0);

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long sum = 0L;
		for (LongWritable value : values) {
			sum += value.get();
		}
		v.set(sum);
		context.write(key,v);
	}
}

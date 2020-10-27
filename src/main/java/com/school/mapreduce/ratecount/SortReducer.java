package com.school.mapreduce.ratecount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-22-9:47
 */
public class SortReducer extends Reducer<SortBean, Text, Text, SortBean> {
	@Override
	protected void reduce(SortBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(value,key);
		}
	}
}

package com.school.mapreduce.topN;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-10-09-9:12
 */
public class TopNReducer extends Reducer<Text, ClassScore, NullWritable, ClassScore> {
	@Override
	protected void reduce(Text key, Iterable<ClassScore> values, Context context) throws IOException, InterruptedException {
		double sum = 0;
		double aver = 0;
		int count = 0;
		for (ClassScore value : values) {
			sum += value.getScore();
			count++;
		}
		aver = sum / count;
		context.write(null, new ClassScore(key.toString(), aver, count));
	}
}

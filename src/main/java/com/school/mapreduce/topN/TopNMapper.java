package com.school.mapreduce.topN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-10-09-8:58
 */
public class TopNMapper extends Mapper<LongWritable, Text, Text, ClassScore> {
	ClassScore classScore = new ClassScore();

	Text text = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		double sum = 0;
		String line = value.toString();
		String[] split = line.split(",");
		classScore.setName(split[0]);
		for(int i = 2; i < split.length; i++){
			sum += Double.parseDouble(split[i]);
		}
		int length = split.length - 2;
		double aver = sum / length;
		classScore.setScore(aver);
		text.set(split[0]);
		context.write(text, classScore);

	}
}

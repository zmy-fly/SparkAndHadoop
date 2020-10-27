package com.school.mapreduce.topN2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-10-09-11:30
 */
public class TopNMapper extends Mapper<LongWritable, Text, Text, CourseScore> {

	CourseScore courseScore;
	Text text = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		double sum = 0;
		String line = value.toString();
		String[] split = line.split(",");
		String cName = split[0];
		String sName = split[1];
		for(int i = 2; i < split.length ; i++){
			sum += Double.parseDouble(split[i]);
		}
		int all = split.length - 2;
		double aver = sum / all;

		String format = String.format("%.1f", aver);
		double result = Double.parseDouble(format);

		courseScore = new CourseScore(cName, result, sName);
		text.set(cName);
//
		context.write(text, courseScore);
	}

}

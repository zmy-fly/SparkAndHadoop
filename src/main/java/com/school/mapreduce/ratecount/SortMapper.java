package com.school.mapreduce.ratecount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-22-9:47
 */
public class SortMapper extends Mapper<LongWritable, Text, SortBean, Text> {

	SortBean sortBean = new SortBean();
	Text text = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String s = value.toString();
		String[] split = s.split("\t");
		long wordNum = Long.parseLong(split[1]);
		String word = split[0];

		sortBean.setWordNum(wordNum);
		text.set(word);

		context.write(sortBean, text);
	}

}

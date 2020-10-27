package com.school.mapreduce.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-24-9:11
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
	String name;
	TableBean tb = new TableBean();
	Text text = new Text();
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) context.getInputSplit();
		name = split.getPath().getName();

	}
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		if(name.startsWith("order")){
			String[] split = line.split("\t");
			tb.setOrderId(split[0]);
			tb.setpId(split[1]);
			tb.setAmount(Integer.parseInt(split[2]));
			tb.setpName("");
			tb.setFlag("order");
			text.set(split[1]);
		}else {
			String[] split = line.split("\t");
			tb.setpId(split[0]);
			System.out.println(split[1]);
			tb.setpName(split[1]);
			tb.setAmount(0);
			tb.setFlag("pd");
			tb.setOrderId("");
			text.set(split[0]);
		}
		context.write(text,tb);
	}
}

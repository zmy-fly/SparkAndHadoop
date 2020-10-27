package com.school.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Bonze
 * @create 2020-09-25-8:57
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	Map<String, String> pdMap = new HashMap<>();
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		URI[] cacheFiles = context.getCacheFiles();
		String path = cacheFiles[0].getPath();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
		String line;
		while(StringUtils.isNotEmpty(line = br.readLine())){
			String[] fields = line.split("\t");
			pdMap.put(fields[0],fields[1]);
		}
		br.close();
	}
	Text k = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String fields[] = line.split("\t");
		String pId = fields[1];
		String pdName = pdMap.get(pId);
		k.set(line + "\t" + pdName);
		context.write(k,NullWritable.get());
	}
}

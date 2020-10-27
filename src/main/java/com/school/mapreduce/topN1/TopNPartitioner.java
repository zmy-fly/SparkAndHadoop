package com.school.mapreduce.topN1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Bonze
 * @create 2020-10-09-11:59
 */
public class TopNPartitioner extends Partitioner<CourseScore, NullWritable>{

	@Override
	public int getPartition(CourseScore courseScore, NullWritable nullWritable, int i) {
		switch (courseScore.getName()){
			case "computer" : return 0;
			case "english" : return 1;
			case "algorithm" : return 2;
			case "math" : return 3;
			default: return 0;
		}
	}
}

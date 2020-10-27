package com.school.mapreduce.topN2;

import com.sun.prism.shader.Texture_LinearGradient_REFLECT_AlphaTest_Loader;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-10-09-11:43
 */

public class TopNReducer extends Reducer<Text, CourseScore, CourseScore, NullWritable>{

	CourseScore courseScore = new CourseScore();
	@Override
	protected void reduce(Text key, Iterable<CourseScore> values, Context context) throws IOException, InterruptedException {
		double s = 0;
		for (CourseScore value : values) {
			if(value.getScore() > s){
				s = value.getScore();
				courseScore.setName(value.getName());
				courseScore.setSname(value.getSname());
				courseScore.setScore(value.getScore());
			}
		}
		context.write(courseScore, NullWritable.get());

	}
}

package com.school.spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author Bonze
 * @create 2020-09-23-8:50
 */
public class JavaWordCount {
	public static void main(String[] args) throws IOException {

		args = new String[]{"Text.txt"};
		SparkConf conf = new SparkConf();
		conf.setMaster("local[1]");
		conf.setAppName(JavaWordCount.class.getSimpleName());
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile(args[0]);

		JavaRDD<String> flatedData = lines.flatMap(	s -> Arrays.asList(s.split(" ")).iterator());
		JavaPairRDD<String, Integer> wordAndOne = flatedData.mapToPair(s -> new Tuple2(s, 1));
		JavaPairRDD<String, Integer> groupData = wordAndOne.reduceByKey((v1, v2) -> v1 + v2);
		JavaPairRDD<Integer, String> beforeSorted = groupData.mapToPair(tuple -> tuple.swap());
		JavaPairRDD<Integer, String> sortedData = beforeSorted.sortByKey(false);
		JavaPairRDD<String, Integer> result = sortedData.mapToPair(t -> t.swap());

		File file = new File("D:\\HdfsClientDemo\\output");
		if (file.isDirectory()) {
			FileUtils.deleteDirectory(file);

			result.saveAsTextFile(file.toString());
			jsc.stop();

		}
	}
}

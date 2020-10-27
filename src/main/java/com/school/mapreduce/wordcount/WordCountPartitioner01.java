package com.school.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Bonze
 * @create 2020-09-18-10:54
 */
public class WordCountPartitioner01 extends Partitioner<Text, IntWritable> {

    public int getPartition(Text text, IntWritable intWritable, int i) {
        double v = Math.random() * 5;
        return (int) v;

    }
}

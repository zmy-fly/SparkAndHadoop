package com.school.mapreduce.topN;

import org.apache.hadoop.io.Writable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-10-09-8:48
 */
public class ClassScore implements Writable{
	private String name;
	private double score;
	private int count;


	public ClassScore(String name, double score, int count) {
		this.count = count;
		this.name = name;
		this.score = score;
	}

	@Override
	public String toString() {
		return name + "\t" +
				score + "\t" +
				count + "\t" ;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;

	}

	public ClassScore() {
	}


	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(name);
		dataOutput.writeInt(count);
		dataOutput.writeDouble(score);
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.name = dataInput.readUTF();
		this.count = dataInput.readInt();
		this.score = dataInput.readDouble();
	}
}
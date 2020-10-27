package com.school.mapreduce.topN2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-10-09-8:48
 */
public class CourseScore implements WritableComparable<CourseScore> {
	private String name;
	private double score;
	private String sname;

	public String getSname() {
		return sname;
	}

	public void setSname(String sname) {
		this.sname = sname;
	}

	public CourseScore(String name, double score, String sname) {
		this.name = name;
		this.score = score;
		this.sname = sname;
	}


	@Override
	public String toString() {
		return sname + "\t" +
				name + "\t" +
				score + "\t" ;
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

	public CourseScore() {
	}


	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(name);
		dataOutput.writeDouble(score);
		dataOutput.writeUTF(sname);
	}


	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.name = dataInput.readUTF();
		this.score = dataInput.readDouble();
		this.sname = dataInput.readUTF();
	}

	@Override
	public int compareTo(CourseScore courseScore) {
		if(this.getName().equals(courseScore.getName())){
			return Double.compare(courseScore.score,this.score);
		}else {
			return this.getName().compareTo(courseScore.getName());
		}

	}
}
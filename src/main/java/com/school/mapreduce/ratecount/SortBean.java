package com.school.mapreduce.ratecount;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-22-9:47
 */
public class SortBean implements WritableComparable<SortBean> {

	private long wordNum;

	public SortBean() {
	}

	public long getWordNum() {
		return wordNum;
	}

	@Override
	public String toString() {
		return "wordNum=" + wordNum;
	}

	public void setWordNum(long wordNum) {
		this.wordNum = wordNum;
	}

	public SortBean(long wordNum) {
		this.wordNum = wordNum;
	}
// 2 7 3
	// 7 3 2
	public int compareTo(SortBean sortBean) {
		return (int) (sortBean.wordNum - this.wordNum);
	}

	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(wordNum);
	}

	public void readFields(DataInput dataInput) throws IOException {
		this.wordNum = dataInput.readLong();
	}

}

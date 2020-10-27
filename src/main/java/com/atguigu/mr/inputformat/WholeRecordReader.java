package com.atguigu.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.naming.Context;
import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-20-23:50
 */
public class WholeRecordReader extends RecordReader<Text, ByteWritable> {
	private Configuration configuration;
	private FileSplit split;

	private boolean isProgress = true;
	private BytesWritable value = new BytesWritable();
	private Text k = new Text();

	public WholeRecordReader() {
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		this.split = (FileSplit) split;
		configuration = new Configuration();
	}

	public boolean nextKeyValue() throws IOException, InterruptedException {

		if(isProgress){
			byte[] contents = new byte[(int)split.getLength()];

			FileSystem fs = null;
			FSDataInputStream fis = null;
			try {
				//获取文件系统
				Path path = split.getPath();
				fs = path.getFileSystem(configuration);

				//3读取数据
				fis = fs.open(path);
				//4读取文件内容
				IOUtils.readFully(fis, contents,0,contents.length);
				//5输出文件内容
				value.set(contents,0,contents.length);
				//6获取文件路径及名称
				String name = split.getPath().toString();
				//7设置输出的key值
				k.set(name);
			} finally {
				IOUtils.closeStream(fis);
			}
			isProgress = false;
			return true;
		}
		return false;
	}

	public Text getCurrentKey() throws IOException, InterruptedException {
		return null;
	}

	public ByteWritable getCurrentValue() throws IOException, InterruptedException {
		return null;
	}

	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	public void close() throws IOException {

	}
}

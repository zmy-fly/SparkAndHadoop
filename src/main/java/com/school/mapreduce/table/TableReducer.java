package com.school.mapreduce.table;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Bonze
 * @create 2020-09-24-9:51
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
		List<TableBean> orderBeans = new ArrayList<>();
		TableBean pdBean = new TableBean();
		for (TableBean value : values) {
			if("order".equals(value.getFlag())){
				TableBean orderBean = new TableBean();
				try {
					BeanUtils.copyProperties(orderBean, value);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
				orderBeans.add(orderBean);
			}else{
				try {
					BeanUtils.copyProperties(pdBean,value);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}
		}
		for (TableBean orderBean : orderBeans) {
			orderBean.setpName(pdBean.getpName());
			context.write(orderBean,NullWritable.get());
		}
	}


}

package com.atguigu.mr.flowsum;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-09-20-17:15
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    FlowBean v = new FlowBean();


    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_downFlow = 0;
        //1 累加求和
        for (FlowBean flowBean : values) {
            sum_upFlow += flowBean.getUpFlow();
            sum_downFlow += flowBean.getDownFlow();
            System.out.println(sum_downFlow + " " + sum_downFlow);
        }
        v.set(sum_upFlow,sum_downFlow);
        context.write(key,v);
    }
}

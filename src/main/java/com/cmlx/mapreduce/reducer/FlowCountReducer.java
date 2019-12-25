package com.cmlx.mapreduce.reducer;

import com.cmlx.mapreduce.pojo.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Desc
 * @Author cmlx
 * @Date 2019-12-24 0024 20:33
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long sum_upFlow = 0;
        long sum_downFLow = 0;

        //1 遍历所用bean，将其中的上行流量，下行流量分别累加
        for (FlowBean flowBean : values) {
            sum_upFlow += flowBean.getUpFlow();
            sum_downFLow += flowBean.getDownFlow();
        }

        //2 封装对象
        FlowBean resultBean = new FlowBean(sum_upFlow, sum_downFLow);

        //3 写出
        context.write(key, resultBean);
    }
}

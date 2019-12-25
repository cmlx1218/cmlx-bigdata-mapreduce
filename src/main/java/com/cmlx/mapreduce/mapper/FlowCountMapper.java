package com.cmlx.mapreduce.mapper;

import com.cmlx.mapreduce.pojo.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Desc
 * @Author cmlx
 * @Date 2019-12-24 0024 20:19
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    FlowBean v = new FlowBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1 获取一行
        String line = value.toString();

        //2 切割字段
        String[] fields = line.split("\t");

        //3 封装对象
        // 取出手机号
        String phoneNumber = fields[1];

        // 取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        k.set(phoneNumber);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);

        //4 写出
        context.write(k, v);


    }
}

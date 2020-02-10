package com.cmlx.mapreduce.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Desc
 * @Author cmlx
 * @Date 2019-12-27 0027 11:01
 */
public class KeyValueMapper extends Mapper<Text, Text, Text, LongWritable> {

    //1 设置value
    LongWritable v = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //2 写出
        context.write(key, v);
    }
}

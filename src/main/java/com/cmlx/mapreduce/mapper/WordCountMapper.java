package com.cmlx.mapreduce.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Desc map阶段
 * KEYIN：输入数据的key
 * VALUEIN：输入数据的value
 * KEYOUT：输出数据的key
 * VALUEOUT：输出数据的value
 * --
 * @Author cmlx
 * @Date 2019-12-23 0023 15:56
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    /**
     * MapTask对每一个<K,V>调用一次
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
//        atguigu atguigu
//        ss ss
//        cls cls
//        jiao
//        banzhang
//        xue
//        hadoop

        //1、获取一行
        String line = value.toString();

        //2、切割单词
        String[] words = line.split(" ");

        //3、循环写出
        for (String word : words) {

            k.set(word);

            context.write(k, v);

        }
    }
}

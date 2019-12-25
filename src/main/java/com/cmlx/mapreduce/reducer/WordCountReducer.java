package com.cmlx.mapreduce.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Desc KEYIN, VALUEIN   map阶段输出的key和value
 * @Author cmlx
 * @Date 2019-12-23 0023 15:57
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable v = new IntWritable();

    /**
     * ReduceTask进程对每一组相同K的<K,V>组调用一次reduce()方法
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        //1、累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }

        v.set(sum);
        //2、写出
        context.write(key, v);

    }

    private <Fuck> Fuck fucker(Fuck fuck) {
        return fuck;
    }
}

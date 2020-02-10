package com.cmlx.mapreduce.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Desc
 * @Author cmlx
 * @Date 2019-12-27 0027 15:56
 */
public class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    LongWritable v = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long sum = 0L;

        //1 汇总
        for (LongWritable value : values) {
            sum += value.get();
        }

        v.set(sum);

        //2 输出
        context.write(key, v);

    }
}

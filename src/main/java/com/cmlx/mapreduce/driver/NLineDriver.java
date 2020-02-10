package com.cmlx.mapreduce.driver;

import com.cmlx.mapreduce.mapper.NLineMapper;
import com.cmlx.mapreduce.reducer.NLineReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

import java.io.IOException;

/**
 * @Desc
 * @Author cmlx
 * @Date 2019-12-27 0027 15:56
 */
public class NLineDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1 获取job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //7 设置每个切片InputSplit中划分三条记录
        NLineInputFormat.setNumLinesPerSplit(job, 3);

        //8 使用NLineInputFormat处理记录数
        job.setInputFormatClass(NLineInputFormat.class);

        //2 这是jar包位置，关联mapper和reducer
        job.setJarByClass(NLineDriver.class);
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

        //3 设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //4 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //5 设置输入输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(args[1]));

        //6 提交job
        job.waitForCompletion(true);

    }

}

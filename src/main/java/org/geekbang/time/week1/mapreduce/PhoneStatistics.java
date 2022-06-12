package org.geekbang.time.week1.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author haoshuaistart
 * @create 2022-05-11 17:37
 */
public class PhoneStatistics {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(PhoneStatistics.class);

        job.setMapperClass(PhoneMap.class);
        job.setReducerClass(PhoneReduce.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

//Map
class PhoneMap extends Mapper<LongWritable,Text,Text,PhoneBean>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        context.write(new Text(fields[1]),new PhoneBean(Integer.parseInt(fields[8]),Integer.parseInt(fields[9])));
    }
}

//Reduce
class PhoneReduce extends Reducer<Text,PhoneBean,Text,PhoneBean>{
    @Override
    protected void reduce(Text key, Iterable<PhoneBean> values, Context context) throws IOException, InterruptedException {

        PhoneBean phoneBean = new PhoneBean();
        Integer upTraffic = 0;
        Integer downTraffic = 0;
        for (PhoneBean value : values) {
            upTraffic += value.getUpTraffic();
            downTraffic += value.getDownTraffic();
        }
        Integer totalTraffic = upTraffic + downTraffic;
        phoneBean.setUpTraffic(upTraffic);
        phoneBean.setDownTraffic(downTraffic);
        phoneBean.setTotalTraffic(totalTraffic);

        context.write(key,phoneBean);
    }
}
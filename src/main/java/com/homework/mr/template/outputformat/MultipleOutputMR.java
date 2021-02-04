package com.homework.mr.template.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MultipleOutputMR {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 如果代码在hadoop集群中运行，请放开以下三行， 然后修改对应的数据输入输出路径
        conf.addResource("hadoop_ha_config/core-site.xml");
        conf.addResource("hadoop_ha_config/hdfs-site.xml");
        System.setProperty("HADOOP_USER_NAME", "bigdata");
        Job job = Job.getInstance(conf);

        job.setJarByClass(MultipleOutputMR.class);

        job.setMapperClass(MultipleOutputMRMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 设置使用自定义的OutputFormat组件
        job.setOutputFormatClass(MultiOutputFormat.class);

        // 设置输入输出
        FileInputFormat.setInputPaths(job, "/mrdata/student_scores/input/");
        // 这并不是真正的结果输出目的地
        Path outPath = new Path("/mrdata/student_scores/output_success");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        // 提交任务
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion ? 0 : 1);
    }

    static class MultipleOutputMRMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Text outkey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {

            // 这段代码求 平均成绩
            String[] splits = value.toString().split(",");
            double totalScore = 0;
            for (int i = 2; i < splits.length; i++) {
                totalScore += Integer.parseInt(splits[i]);
            }
            double avg_score = totalScore / (splits.length - 2);

            // 最终成绩，所有考试次数的平均成绩
            long avg_score_long = Math.round(avg_score);

            if (avg_score_long >= 60) {
                outkey.set("1::" + (splits[0] + "\t" + splits[1] + "\t" + avg_score_long));
            } else {
                outkey.set("2::" + (splits[0] + "\t" + splits[1] + "\t" + avg_score_long));
            }

            context.write(outkey, NullWritable.get());
        }
    }
}
package com.homework.mr.template.udgroup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * TopN问题
 */
public class ClazzScoreTop1InfoMR {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 如果代码在hadoop集群中运行，请放开以下三行， 然后修改对应的数据输入输出路径

        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Job job = Job.getInstance(conf);

        job.setJarByClass(ClazzScoreTop1InfoMR.class);

        job.setMapperClass(ScoreTop1MRMapper.class);
        job.setReducerClass(ScoreTop1MRReducer.class);

        job.setOutputKeyClass(ClazzScore.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置传入reducer的数据分组规则
        job.setGroupingComparatorClass(ClazzScoreGroupComparator.class);

        // 设置 MapReduce 程序输入输出
        Path inputPath = new Path("D:\\fpy\\study\\奈学\\homework\\hwhadoop1\\src\\main\\java\\com\\homework\\mr\\hw\\groupsort\\score.log");
        Path outputPath = new Path("./mrdata/score/output/");
        FileInputFormat.setInputPaths(job, inputPath);
        FileSystem fs = FileSystem.newInstance(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        // 提交任务
        boolean status = job.waitForCompletion(true);
        System.exit(status ? 0 : 1);
    }

    static class ScoreTop1MRMapper extends Mapper<LongWritable, Text, ClazzScore, NullWritable> {
        ClazzScore cs = new ClazzScore();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {

            String[] splits = value.toString().split(",");
            cs.set(splits[0], splits[1], Double.parseDouble(splits[2]));
            context.write(cs, NullWritable.get());
        }
    }

    static class ScoreTop1MRReducer extends Reducer<ClazzScore, NullWritable, ClazzScore, NullWritable> {

        @Override
        protected void reduce(ClazzScore cs, Iterable<NullWritable> scores, Context context) throws IOException, InterruptedException {
            // 按照规则，取每组的第一个就是Top1

            //应该是这个样子的写 所有排序
           /* for (NullWritable score : scores) {
                context.write(cs, NullWritable.get());
            }*/


            // 现在要求 top 3  每个分组写3次

            int count = 0;
            for (NullWritable score : scores) {
                if(count >=3){
                    break;
                }
                context.write(cs, NullWritable.get());
                count++;
            }
        }
    }
}
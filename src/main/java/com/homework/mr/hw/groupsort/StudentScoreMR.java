package com.homework.mr.hw.groupsort;

import com.homework.mr.template.udgroup.ClazzScoreTop1InfoMR;
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
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/28.
 */
public class StudentScoreMR {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = new Configuration();
        // 如果代码在hadoop集群中运行，请放开以下三行， 然后修改对应的数据输入输出路径

        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Job job = Job.getInstance(conf);

        job.setJarByClass(ClazzScoreTop1InfoMR.class);

        job.setMapperClass(StuScoreMapper.class);
        job.setReducerClass(StuScoreReducer.class);

        job.setOutputKeyClass(StudentScore.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置传入reducer的数据分组规则
        job.setGroupingComparatorClass(MyStudentScoreGroup.class);

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


    /**
     * computer,zs,56
     *
     */
    public static class StuScoreMapper extends Mapper<LongWritable, Text,StudentScore, NullWritable> {

        StudentScore stu = new StudentScore();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] split = line.split(",");

            stu.setSubject(split[0]);
            stu.setName(split[1]);
            stu.setScore(Double.parseDouble(split[2]));

            context.write(stu, NullWritable.get());

        }
    }

        public static class StuScoreReducer extends Reducer<StudentScore,NullWritable,StudentScore,NullWritable>{

            @Override
            protected void reduce(StudentScore key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

                //求 top3

                // 现在要求 top 3  每个分组写3次

                int count = 0;
                for (NullWritable score : values) {
                    if(count >=5){
                        break;
                    }
                    context.write(key, NullWritable.get());
                    count++;
                }


            }
        }




}

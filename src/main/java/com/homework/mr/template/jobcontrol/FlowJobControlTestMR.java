package com.homework.mr.template.jobcontrol;

import com.homework.mr.template.flow.FlowBean;
import com.homework.mr.template.flow.FlowMR1_Sum;
import com.homework.mr.template.flow.FlowMR3_Sort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowJobControlTestMR {

    public static void main(String[] args) throws Exception {

        // 第一个任务
        Configuration conf1 = new Configuration();
        // 如果代码在hadoop集群中运行，请放开以下三行， 然后修改对应的数据输入输出路径
        conf1.addResource("hadoop_ha_config/core-site.xml");
        conf1.addResource("hadoop_ha_config/hdfs-site.xml");
        System.setProperty("HADOOP_USER_NAME", "bigdata");
        Job jobsum = Job.getInstance(conf1);
        jobsum.setJarByClass(FlowJobControlTestMR.class);
        jobsum.setMapperClass(FlowMR1_Sum.FlowMR1SumMapper.class);
        jobsum.setReducerClass(FlowMR1_Sum.FlowMR1SumReducer.class);
        jobsum.setMapOutputKeyClass(Text.class);
        jobsum.setMapOutputValueClass(FlowBean.class);
        jobsum.setOutputKeyClass(NullWritable.class);
        jobsum.setOutputValueClass(FlowBean.class);
        Path inputPath = new Path("/mrdata/flow/input");
        FileInputFormat.setInputPaths(jobsum, inputPath);
        // 告诉框架，我们的处理结果要输出到哪里去
        Path outputPath = new Path("/mrdata/flow/output1_sum");
        FileSystem fs = FileSystem.get(conf1);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(jobsum, outputPath);

        // 第二个任务
        Configuration conf2 = new Configuration();
        // 如果代码在hadoop集群中运行，请放开以下三行， 然后修改对应的数据输入输出路径
        conf1.addResource("hadoop_ha_config/core-site.xml");
        conf1.addResource("hadoop_ha_config/hdfs-site.xml");
        System.setProperty("HADOOP_USER_NAME", "bigdata");
        Job jobsort = Job.getInstance(conf2);
        jobsort.setJarByClass(FlowJobControlTestMR.class);
        jobsort.setMapperClass(FlowMR3_Sort.FlowMR3SortMapper.class);
        jobsort.setReducerClass(FlowMR3_Sort.FlowMR3SortReducer.class);
        jobsort.setMapOutputKeyClass(FlowBean.class);
        jobsort.setMapOutputValueClass(NullWritable.class);
        jobsort.setOutputKeyClass(NullWritable.class);
        jobsort.setOutputValueClass(FlowBean.class);
        Path inputPath2 = new Path("/mrdata/flow/output1_sum");
        FileInputFormat.setInputPaths(jobsort, inputPath2);
        // 告诉框架，我们的处理结果要输出到哪里去
        Path outputPath2 = new Path("/mrdata/flow/output3_sort");
        if (fs.exists(outputPath2)) {
            fs.delete(outputPath2, true);
        }
        FileOutputFormat.setOutputPath(jobsort, outputPath2);

        // 把普通Job对象转换成ControlledJob对象，方便最后的JobControl对象进行管理
        ControlledJob cjsum = new ControlledJob(jobsum.getConfiguration());
        cjsum.setJob(jobsum);
        ControlledJob cjsort = new ControlledJob(jobsort.getConfiguration());
        cjsort.setJob(jobsort);

        // 新建一个JobControl对象管理一组job对象
        JobControl jc = new JobControl("sum and sort flow");
        jc.addJob(cjsum);
        jc.addJob(cjsort);

        // 最重要的一句代码：设置各个mapreduce job之间的依赖关系
        cjsort.addDependingJob(cjsum);

        // 把jobcontrol对象放入到一个线程里去跑
        Thread t = new Thread(jc);
        t.start();

        // 主线程监控任务是否完成
        while (!jc.allFinished()) {
            Thread.sleep(1000);
        }
        // 完成则退出执行jobctrol一串job任务的线程
        jc.stop();
        System.exit(0);
    }
}
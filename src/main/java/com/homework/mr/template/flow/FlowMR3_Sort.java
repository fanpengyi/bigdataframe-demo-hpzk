package com.homework.mr.template.flow;

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

public class FlowMR3_Sort {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 如果代码在hadoop集群中运行，请放开以下三行， 然后修改对应的数据输入输出路径
       /* conf.addResource("hadoop_ha_config/core-site.xml");
        conf.addResource("hadoop_ha_config/hdfs-site.xml");*/
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowMR3_Sort.class);

        job.setMapperClass(FlowMR3SortMapper.class);
        job.setReducerClass(FlowMR3SortReducer.class);

//        // 如果没有reduce阶段 就不会排序的。但是我就是想排序！
//        job.setSortComparatorClass();

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 如果想全局排序，reduceTask只能有一个。
        // 但是：数量太大，一个reduceTask怎么能实现有序呢？这是很困难的。
        //job.setNumReduceTasks(1);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FlowBean.class);

        // 告诉框架，我们要处理的文件在哪个路径下
        Path inputPath = new Path("./mrdata/flow/output2_combiner");
        FileInputFormat.setInputPaths(job, inputPath);
        // 告诉框架，我们的处理结果要输出到哪里去
        Path outputPath = new Path("./mrdata/flow/output3_sort");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean status = job.waitForCompletion(true);
        System.exit(status ? 0 : 1);
    }

    public static class FlowMR3SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {
        FlowBean outkey = new FlowBean();

        /**
         * value = 13602846565     26860680        40332600        67193280
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {

            String[] splits = value.toString().split("\t");

            String phone = splits[0];
            long upflow = Long.parseLong(splits[1]);
            long downflow = Long.parseLong(splits[2]);
            // long sumflow = Long.parseLong(splits[3]);
            outkey.set(phone.toString(), upflow, downflow);

            // mapper阶段输出数据
            context.write(outkey, NullWritable.get());
        }
    }

    public static class FlowMR3SortReducer extends Reducer<FlowBean, NullWritable, NullWritable, FlowBean> {
        @Override
        protected void reduce(FlowBean flow, Iterable<NullWritable> nvls, Context context) throws IOException,
                InterruptedException {

            for (NullWritable t : nvls) {
                context.write(NullWritable.get(), flow);
            }
        }
    }
}

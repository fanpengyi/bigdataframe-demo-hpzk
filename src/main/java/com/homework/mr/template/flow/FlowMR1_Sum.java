package com.homework.mr.template.flow;

import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 *
 *     执行日志： 不带 combiner
 *      Map-Reduce Framework
 * 		Map input records=304920
 * 		Map output records=304920
 * 		Map output bytes=14857920
 * 		Map output materialized bytes=15467766
 * 		Input split bytes=162
 * 		Combine input records=0
 * 		Combine output records=0
 * 		Reduce input groups=21
 * 		Reduce shuffle bytes=15467766
 * 		Reduce input records=304920
 * 		Reduce output records=21
 * 		Spilled Records=609840
 * 		Shuffled Maps =1
 * 		Failed Shuffles=0
 * 		Merged Map outputs=1
 * 		GC time elapsed (ms)=33
 * 		Total committed heap usage (bytes)=915406848
 *
 */
public class FlowMR1_Sum {

    // 在kv中传输我们自定义的对象是可以的，但是必须实现hadoop的序列化机制 implements Writable,
    // 如果要排序，还要实现Comparable接口，hadoop为我们提供了一个方便的类，叫做WritableComparable，直接实现就好
    public static class FlowMR1SumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        Text k = new Text();
        FlowBean v = new FlowBean();

        /**
         * value = 1363157986041 	13480253104	5C-0E-8B-C7-FC-80:CMCC-EASY	120.197.40.4			3	3	180	180	200
         * @param key
         * @param value
         * @param context
         * @throws InterruptedException
         * @throws IOException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws InterruptedException,
                IOException {
            // 将读到的一行数据进行字段切分
            String line = value.toString();
            String[] fields = StringUtils.split(line, "\t");

            // 抽取业务所需要的各字段
            String phone = fields[1];
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long downFlow = Long.parseLong(fields[fields.length - 2]);

            k.set(phone);
            v.set(phone, upFlow, downFlow);
            context.write(k, v);
        }
    }

    public static class FlowMR1SumReducer extends Reducer<Text, FlowBean, NullWritable, FlowBean> {
        FlowBean v = new FlowBean();

        // reduce方法接收到的key是某一组<a手机号，bean><a手机号，bean><a手机号，bean>中的第一个手机号，
        // reduce方法接收到的vlaues是这一组kv中的所有bean的一个迭代器
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws InterruptedException,
                IOException {
            long upFlowCount = 0;
            long downFlowCount = 0;
            for (FlowBean bean : values) {
                upFlowCount += bean.getUpFlow();
                downFlowCount += bean.getDownFlow();
            }
            v.set(key.toString(), upFlowCount, downFlowCount);
            context.write(NullWritable.get(), v);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 如果代码在hadoop集群中运行，请放开以下三行， 然后修改对应的数据输入输出路径
       /* conf.addResource("hadoop_ha_config/core-site.xml");
        conf.addResource("hadoop_ha_config/hdfs-site.xml");*/
        //conf.set("fs.defaultFS", "hdfs://hadoop-80:9000");
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Job job = Job.getInstance(conf);
        // 告诉框架，我们的程序所在jar包的路径
        job.setJarByClass(FlowMR1_Sum.class);

        // 告诉框架，我们的程序所用的mapper类和reducer类
        job.setMapperClass(FlowMR1SumMapper.class);
        job.setReducerClass(FlowMR1SumReducer.class);

        // 告诉框架，我们的mapperreducer输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 如果map阶段输出的数据类型跟最终输出的数据类型一致，就只要以下两行代码来指定，上面两行可注释
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FlowBean.class);

        // 框架中默认的输入输出组件就是这俩货，所以可以省略这两行代码
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 告诉框架，我们要处理的文件在哪个路径下
        //Path inputPath = new Path("/mrdata/flow/input");
        Path inputPath = new Path("D:\\fpy\\study\\奈学\\homework\\hwhadoop1\\src\\main\\java\\com\\homework\\mr\\template\\flow\\flow.log");
        FileInputFormat.setInputPaths(job, inputPath);
        // 告诉框架，我们的处理结果要输出到哪里去
        Path outputPath = new Path("/mrdata/flow/output1_sum");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);



        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
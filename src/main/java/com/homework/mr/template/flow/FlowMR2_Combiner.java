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

/**
 * @author 马中华 奈学教育
 * 手机号 上行流量 下行流量 总流量
 */
public class FlowMR2_Combiner {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 如果代码在hadoop集群中运行，请放开以下三行， 然后修改对应的数据输入输出路径
        conf.addResource("hadoop_ha_config/core-site.xml");
        conf.addResource("hadoop_ha_config/hdfs-site.xml");
        System.setProperty("HADOOP_USER_NAME", "bigdata");

        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowMR2_Combiner.class);

        job.setMapperClass(FlowMR2Mapper.class);
        job.setReducerClass(FlowMR2Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 设置 Combiner
        job.setCombinerClass(FlowMR2Combiner.class);
        //job.setCombinerClass(FlowExercise2Reducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // 告诉框架，我们要处理的文件在哪个路径下
        Path inputPath = new Path("D:\\fpy\\study\\奈学\\homework\\hwhadoop1\\src\\main\\java\\com\\homework\\mr\\template\\flow\\flow.log");
        FileInputFormat.setInputPaths(job, inputPath);
        // 告诉框架，我们的处理结果要输出到哪里去
        Path outputPath = new Path("./mrdata/flow/output2_combiner_old");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        // 提交任务
        boolean status = job.waitForCompletion(true);
        System.exit(status ? 0 : 1);
    }

    public static class FlowMR2Mapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        Text k = new Text();
        FlowBean v = new FlowBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {
            String[] splits = value.toString().split("\t");

            String phone = splits[1];
            long upFlow = Long.parseLong(splits[8]);
            long downFlow = Long.parseLong(splits[9]);

            k.set(phone);
            v.set(phone, upFlow, downFlow);
            context.write(k, v);
        }
    }

    public static class FlowMR2Reducer extends Reducer<Text, FlowBean, NullWritable, FlowBean> {
        FlowBean v = new FlowBean();

        @Override
        protected void reduce(Text phone, Iterable<FlowBean> flows, Context context) throws IOException,
                InterruptedException {

            // 该phone用户的总上行流量
            long sumUpflow = 0;
            long sumDownflow = 0;
            for (FlowBean f : flows) {
                sumUpflow += f.getUpFlow();
                sumDownflow += f.getDownFlow();
            }
            v.set(phone.toString(), sumUpflow, sumDownflow);
            context.write(NullWritable.get(), v);
        }
    }

    public static class FlowMR2Combiner extends Reducer<Text, FlowBean, Text, FlowBean> {
        FlowBean v = new FlowBean();
        @Override
        protected void reduce(Text phone, Iterable<FlowBean> flows, Context context) throws IOException,
                InterruptedException {

            // 该phone用户的总上行流量
            long sumUpflow = 0;
            long sumDownflow = 0;
            for (FlowBean f : flows) {
                sumUpflow += f.getUpFlow();
                sumDownflow += f.getDownFlow();
            }
            v.set(phone.toString(), sumUpflow, sumDownflow);
            context.write(phone, v);
        }
    }
}

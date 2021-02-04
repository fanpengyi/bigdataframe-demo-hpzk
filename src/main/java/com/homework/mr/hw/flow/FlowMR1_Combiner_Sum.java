package com.homework.mr.hw.flow;

import com.homework.mr.template.flow.FlowBean;
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
 * @date 2021/1/27.
 *
 *  统计每个 手机号 的 上行流量 下行流量 总流量
 *
 *
 *
 */

/**
 *
 * Map-Reduce Framework
 * 		Map input records=304920
 * 		Map output records=304920
 * 		Map output bytes=14857920
 * 		Map output materialized bytes=1071
 * 		Input split bytes=162
 * 		Combine input records=304920
 * 		Combine output records=21
 * 		Reduce input groups=21
 * 		Reduce shuffle bytes=1071
 * 		Reduce input records=21
 * 		Reduce output records=21
 * 		Spilled Records=42
 * 		Shuffled Maps =1
 * 		Failed Shuffles=0
 * 		Merged Map outputs=1
 * 		GC time elapsed (ms)=35
 * 		Total committed heap usage (bytes)=915406848
 *
 */
public class FlowMR1_Combiner_Sum {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        System.setProperty("HADOOP_USER_NAME","hadoop");


        Job job = Job.getInstance(conf);

        //设置 主类的 calss
        job.setJarByClass(FlowMR1_Combiner_Sum.class);

        job.setMapperClass(FlowMR2Mapper.class);
        job.setReducerClass(FlowMR2Reducer.class);

        //mapTask 输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 设置 combiner
        job.setCombinerClass(FlowMR2Combiner.class);
        //reducer 输出
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //设置文件输入输出路径

        FileInputFormat.setInputPaths(job,new Path("D:\\fpy\\study\\奈学\\homework\\hwhadoop1\\src\\main\\java\\com\\homework\\mr\\template\\flow\\flow.log"));


        //设置文件输出
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path("./mrdata/flow/output2_combiner_sum");

        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }

        FileOutputFormat.setOutputPath(job,outputPath);

        //提交任务
        boolean status = job.waitForCompletion(true);

        System.exit(status ?0:1);


    }

    /**
     * Mapper 程序
     *
     * 数据格式：1 - 手机号 8 -上行流量 9 -下行流量
     * 1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
     *
     */

    public static class FlowMR2Mapper extends Mapper<LongWritable,Text, Text, FlowBean>{

        Text k = new Text();
        FlowBean v = new FlowBean();
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String[] split = value.toString().split("\t");


            String phoneNum = split[1];
            long upFlow = Long.parseLong(split[8]);
            long downFlow = Long.parseLong(split[9]);

            k.set(phoneNum);
            v.set(phoneNum,upFlow,downFlow);

            context.write(k,v);

        }
    }

    /**
     * reducer 程序  flowBean 输出
     *
     */
    public static class FlowMR2Reducer extends Reducer<Text,FlowBean, NullWritable,FlowBean>{

        FlowBean v = new FlowBean();

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

            //计算总流量
             long sumUpFlow = 0;
             long sumDownFlow = 0;

             for(FlowBean fw: values){
                 sumUpFlow += fw.getUpFlow();
                 sumDownFlow += fw.getDownFlow();
             }
            //赋值
             v.set(key.toString(),sumUpFlow,sumDownFlow);

             context.write(NullWritable.get(),v);

        }
    }

    /**
     *
     * mapTask 之后  Reducer 之前
     *
     * 输入 KV (Text,FLowBean)
     * 输出 KV (Text,FLowBean)
     *
     * 其实就是一个 小的 reducer
     */
    public static class FlowMR2Combiner extends Reducer<Text,FlowBean,Text,FlowBean>{

        FlowBean v = new FlowBean();

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

            long sumUpFlow = 0;
            long sumDownFlow = 0;

            for (FlowBean value : values) {
                sumUpFlow += value.getUpFlow();
                sumDownFlow += value.getDownFlow();
            }

            v.set(key.toString(),sumUpFlow,sumDownFlow);

            context.write(key,v);

        }
    }




}

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
 * 计算求和之后 ，进行排序
 *
 * 全局排序-- reduceTask 只能有一个
 *
 * 排序使用 实现 WritableComparable 接口 覆盖 compreTo 方法 (fb.getSumFlow() - this.sumFlow) 降序 额外对象排在前面
 *
 * 注意：只对 key 进行排序，所以需要把 FlowBean 放到 mapper 输出的 key 中
 *
 *  mapTask extends Mapper<LongWritable, Text, FlowBean, NullWritable>
 *
 *
 */
public class FlowMR2_Sum_Sort {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = new Configuration();

        System.setProperty("HADOOP_USER_NAME","hadoop");

        //注册类

        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowMR2_Sum_Sort.class);
        job.setMapperClass(FlowMR3SortMapper.class);
        job.setReducerClass(FlowMR3SortReducer.class);

        // mapTask
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //reduceTask
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FlowBean.class);

        //指定文件目录

        FileInputFormat.setInputPaths(job,new Path("D:\\fpy\\study\\奈学\\homework\\hwhadoop1\\mrdata\\flow\\output2_combiner_sum\\part-r-00000"));

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

            // Sort 只对 key 起作用！
         @Override
         protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             String[] split = value.toString().split("\t");

             String phone = split[0];
             long upFlow = Long.parseLong(split[1]);
             long downFlow = Long.parseLong(split[2]);
             outkey.set(phone,upFlow,downFlow);

             context.write(outkey, NullWritable.get());
         }
     }



    public static class FlowMR3SortReducer extends Reducer<FlowBean,NullWritable, NullWritable,FlowBean>{

        @Override
        protected void reduce(FlowBean flow, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable value : values) {
                context.write(NullWritable.get(),flow);
            }


        }
    }


}

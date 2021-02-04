package com.homework.mr.template.combineFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 作者： 马中华：http://blog.csdn.net/zhongqi2513
 * 日期： 2017年9月19日上午10:34:42
 * 描述： 利用 CombineInputFormat来让一个MapTask可以一次性读取很多的小文件进行处理
 */
public class CombineFileInputFormatMR {

    private static final long ONE_MB = 1024 * 1024L;

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 如果代码在hadoop集群中运行，请注释以下三行
        conf.addResource("hadoop_ha_config/core-site.xml");
        conf.addResource("hadoop_ha_config/hdfs-site.xml");
        System.setProperty("HADOOP_USER_NAME", "bigdata");

        //
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", ONE_MB * 32);
        Job job = Job.getInstance(conf);

        job.setJarByClass(CombineFileInputFormatMR.class);

        job.setMapperClass(CombineFileInputFormatMRMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(CombineTextInputFormat.class);

        Path inputPath = new Path("/mrdata/smallfiles/input");
        Path outputPath = new Path("/mrdata/smallfiles/output2");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean waitForCompletion = job.waitForCompletion(true);

        System.exit(waitForCompletion ? 0 : 1);
    }

    public static class CombineFileInputFormatMRMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text fileNameKey = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            fileNameKey.set(context.getConfiguration().get(MRJobConfig.MAP_INPUT_FILE));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {

            Configuration configuration = context.getConfiguration();
            System.out.println("########" + configuration.get(MRJobConfig.MAP_INPUT_FILE));
            context.write(fileNameKey, value);
        }
    }
}

package com.homework.mr.template.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 手机号 上行流量 下行流量 总流量
 *
 * @author Administrator
 */
public class FlowMR4_Partitioner {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 如果代码在hadoop集群中运行，请放开以下三行， 然后修改对应的数据输入输出路径
        conf.addResource("hadoop_ha_config/core-site.xml");
        conf.addResource("hadoop_ha_config/hdfs-site.xml");
        System.setProperty("HADOOP_USER_NAME", "bigdata");
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowMR4_Partitioner.class);

        job.setMapperClass(FlowMR4Mapper.class);
        job.setReducerClass(FlowMR4Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 这个地方设置的是并行运行的ReduceTask的个数。非常重要。一定要跟实际情况一致。否则程序就报错。
        // 如果你设置的分区组件是 默认的 HashPartitioner 那么不管设置多少个分区都可以 0 1 2
        // 必须最好就是分区的总个数
        job.setNumReduceTasks(5);
        job.setPartitionerClass(PhonePartitioner.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FlowBean.class);

        // 告诉框架，我们要处理的文件在哪个路径下
        Path inputPath = new Path("/mrdata/flow/input");
        FileInputFormat.setInputPaths(job, inputPath);
        // 告诉框架，我们的处理结果要输出到哪里去
        Path outputPath = new Path("/mrdata/flow/output4_partitioner");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean status = job.waitForCompletion(true);
        System.exit(status ? 0 : 1);
    }

    public static class FlowMR4Mapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        Text k = new Text();
        FlowBean v = new FlowBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
            String[] splits = value.toString().split("\t");

            String phone = splits[1];
            long upflow = Long.parseLong(splits[8]);
            long downflow = Long.parseLong(splits[9]);

            k.set(phone);
            v.set(phone.toString(), upflow, downflow);
            context.write(k, v);
        }
    }

    public static class FlowMR4Reducer extends Reducer<Text, FlowBean, NullWritable, FlowBean> {
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

    /**
     * 我抽象了执行逻辑
     *  13544556677  -----> 那个地方？
     *
     *  135开头的，都是北京的，就是 0 号分区
     *  136开头的，都是天津的， 就是 1号分区
     *  ....
     */
    public static class PhonePartitioner extends Partitioner<Text, FlowBean> {

        /**
         * "137" -> 0
         * "138" -> 1
         * "139" -> 2
         * ...
         * 其他  -> 5
         * 总共6个分区
         */
        private static Map<String, Integer> phoneProvinceMap = new HashMap<String, Integer>();

        static {
            phoneProvinceMap.put("135", 0);
            phoneProvinceMap.put("136", 1);
            phoneProvinceMap.put("137", 2);
            phoneProvinceMap.put("138", 3);
            phoneProvinceMap.put("139", 4);
        }


        /**
         * 返回的值，必须是从0开始，最好不中断的递增的
         * // 0 1 2 3 4
         * // 0 1 2 3 4 5 6 7 8 9
         * @param key
         * @param value
         * @param numPartitions
         * @return
         */
        @Override
        public int getPartition(Text key, FlowBean value, int numPartitions) {

            String threePhone = key.toString().substring(0, 3);
            Integer integer = phoneProvinceMap.get(threePhone);
            if (integer != null) {
                return integer;
            } else {
                return 5;
            }
        }
    }
}

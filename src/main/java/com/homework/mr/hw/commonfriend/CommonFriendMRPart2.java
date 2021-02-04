package com.homework.mr.hw.commonfriend;

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
import java.util.Arrays;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/28.
 *
 *
 *
 *
 * 计算共同好友：
 *
 * A:B,C,D,F,E,O
 * B:A,C,E,K
 * C:F,A,D,I
 *
 *
 *
 *
 * 第一步： 计算出 谁的 好友 有A 谁的好友有B 。。
 *
 *  给出的数据格式：<人，List<好友>
 *
 *      需要求出的数据 <好友,List<人>>   有共同好友A 的人有哪些，有共同好友B的人有哪些
 *
 *      map <key: 好友，人> --
 *
 *      第一行 A:B,C,D,F,E,O ==》 (B,A) (C,A) (D,A) (F,A) (E,A) (O,A)
 *      第二行 B:A,C,E,K ==》 (A,B) (C,B) (E,B) (K,B)
 *
 *
 *       reduce <key: 好友，value:人聚合>
 *
 *
 *
 * 第二步：通过 <好友,List<人>> 的 value 两两连接 -->
 *
 *     map <key: 两两连接，value:好友>
 *      reduce <key: 两两连接，value:好友聚合>
 *
 *
 *
 */
public class CommonFriendMRPart2 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        System.setProperty("HADOOP_USER_NAME","hadoop");
        //设置 Class 类

        Job job = Job.getInstance(conf);


        job.setJarByClass(CommonFriendMRPart2.class);
        job.setMapperClass(CommonFriendPart2Mapper.class);
        job.setReducerClass(CommonFriendPart2Reducer.class);

        //MapOutputkey

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);

        //输入路径

        FileInputFormat.setInputPaths(job,new Path("D:\\fpy\\study\\奈学\\homework\\hwhadoop1\\mrdata\\hw\\comfd\\part1\\part-r-00000"));

        FileSystem fs = FileSystem.get(conf);

        Path outPutPath = new Path("./mrdata/hw/comfd/part2");

        if(fs.exists(outPutPath)){
            fs.delete(outPutPath,true);
        }

        FileOutputFormat.setOutputPath(job,outPutPath);

        boolean status = job.waitForCompletion(true);

        System.exit(status ? 0:1);



    }


    /**
     *
     *   A:I,K,C,B,G,F,H,O,D  //有共同好友 A 的所有人
     *   B:A,F,J,E              //有共同好友 B 的所有人
     *   C:A,E,B,H,F,G,K      //有共同好友 C 的所有人
     *
     *   map <key: 人-人，好友> --
     *
     *  *       先排序 A: B,C,D,F,G,H,I,K,O
     *  *      第一行 A:B,C,D,F,E,O ==》 (B-C,A) (B-D,A) (B-F,A) 。。。(C-D,A)
     *  *
     *
     *
     */
    public static class CommonFriendPart2Mapper extends Mapper<LongWritable,Text,Text, Text>{

        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split(":");

            String friend = split[0];

            String users = split[1];

            String[] usersArray = users.split(",");

            Arrays.sort(usersArray);

            v.set(friend);

            for (int i = 0; i < usersArray.length -1; i++) {
                for (int j = i+1; j < usersArray.length; j++) {
                    k.set(usersArray[i] +"-" +usersArray[j]);
                    context.write(k,v);
                }

            }
        }

    }



    public static class CommonFriendPart2Reducer extends Reducer<Text,Text,Text, NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuffer buffer = new StringBuffer();

            for (Text value : values) {
                buffer.append(value+",");
            }

            String result = buffer.toString().substring(0, buffer.toString().length() - 1);

            result = key.toString() +":"+result;

            key.set(result)  ;

            context.write(key,NullWritable.get());

        }
    }








}

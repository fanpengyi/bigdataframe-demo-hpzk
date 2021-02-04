package com.homework.mr.template.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MultiOutputFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(
            TaskAttemptContext job) throws IOException, InterruptedException {

        Configuration configuration = job.getConfiguration();
        FileSystem fs = FileSystem.get(configuration);

        Path p1 = new Path("/mrdata/student_scores/output1/result_gt60");
        Path p2 = new Path("/mrdata/student_scores/output2/result_lt60");
        if(fs.exists(p1)){
            fs.delete(p1, true);
        }
        if(fs.exists(p2)){
            fs.delete(p2, true);
        }

        FSDataOutputStream out1 = fs.create(p1);
        FSDataOutputStream out2 = fs.create(p2);

        return new MyRecordWriter(out1, out2);
    }

    static class MyRecordWriter extends RecordWriter<Text, NullWritable> {

        FSDataOutputStream fsdout = null;
        FSDataOutputStream fsdout1 = null;

        public MyRecordWriter(FSDataOutputStream fsdout, FSDataOutputStream fsdout1) {
            super();
            this.fsdout = fsdout;
            this.fsdout1 = fsdout1;
        }

        /**
         * 这个key，就是你的reducer程序，写出来的结果数据，最终由 RecordWriter组件中的 write 方法执行真正的写出
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         *
         * 参数来自于： reduce组件的输出
         *  context.write(outkey, NullWritable.get());
         *
         *  fsdout: HDFS 文件系统的输出流。  FileInputStream  FileOutputFormat
         */
        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            String[] strs = key.toString().split("::");
            if (strs[0].equals("1")) {
                fsdout.write((strs[1] + "\n").getBytes());
            } else {
                fsdout1.write((strs[1] + "\n").getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            IOUtils.closeStream(fsdout);
            IOUtils.closeStream(fsdout1);
        }
    }
}
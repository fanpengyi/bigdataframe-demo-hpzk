package com.homework.mr.template.join.map;

import com.homework.mr.template.join.pojo.MovieRate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * mapjoin的代码，只能运行在 Hadoop 集群中。所以必须打jar包，通过 hadoop jar 的方式来提交任务运行
 */
public class MoivieRateMapjoinMR {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(MoivieRateMapjoinMR.class);

        // 设置 job 的必备各种组件
        job.setMapperClass(MoivieRateMapjoinMRMapper.class);
        job.setMapOutputKeyClass(MovieRate.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 因为使用 MapSide 的 Join 实现，所以，不需要 reduce
        job.setNumReduceTasks(0);

        // 通过job对象，指定将会缓存到各个将要执行maptask的服务器节点上去的缓存文件的路径，参数要求是一个URI对象
        URI uri = new URI("/mrdata/movierate/movie/movies.dat");
        job.addCacheFile(uri);
        // 以上两句代码实现的效果是：把小文件缓存到了HDFS

        // 设置输入输出:  rate 里面放的数据： ratings.dat 大文件
        Path maxPath = new Path("/mrdata/movierate/rate");
        Path outPath = new Path("/mrdata/movierate/movierate_mapjoin_out");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        FileInputFormat.setInputPaths(job, maxPath);

        // 提交任务
        boolean status = job.waitForCompletion(true);
        System.exit(status ? 0 : 1);
    }

    static class MoivieRateMapjoinMRMapper extends Mapper<LongWritable, Text, MovieRate, NullWritable> {

		// 新建一个map用来存储小表的所有数据，也就意味着把小表的所有数据都加载到内存中了。
        // 这个map用来存放 movies.dat 这个小文件的所有数据。key: movieid,   value: name, type
		private Map<String, String> movieMap = new HashMap<String, String>();

		// 输出key对象，提前初始化
		MovieRate outkey = new MovieRate();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            // 通过context获取所有的缓存文件，并且获取到缓存文件的路径等信息
            Path[] localCacheFiles = context.getLocalCacheFiles();
            URI[] abc = context.getCacheFiles();
            String strPath = localCacheFiles[0].toUri().toString();

            // 自定义读取逻辑去读这个文件，然后进行相关的业务处理
            BufferedReader br = new BufferedReader(new FileReader(strPath));
            String readLine = null;

            // 读取文件数据解析到 movieMap 中。
            while (null != (readLine = br.readLine())) {
                System.out.println(readLine);
                String[] movieFileds = readLine.split("::");
                String movieid = movieFileds[0];
                String moviename = movieFileds[1];
                String movieType = movieFileds[2];

                movieMap.put(movieid, moviename + "::" + movieType);
            }
            IOUtils.closeStream(br);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {

            String[] splits = value.toString().split("::");
            String movieid = splits[1];
            int rate = Integer.parseInt(splits[2]);
            long ts = Long.parseLong(splits[3]);
            String userid = splits[0];

            // map根据key到movieMap中去匹配小表数据
            String movieNameAndType = movieMap.get(movieid);
            String movieName = movieNameAndType.split("::")[0];
            String movieType = movieNameAndType.split("::")[1];

			outkey.set(movieid, userid, rate, movieName, movieType, ts);

            context.write(outkey, NullWritable.get());
        }
    }
}

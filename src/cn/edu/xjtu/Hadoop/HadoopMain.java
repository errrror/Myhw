package cn.edu.xjtu.Hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapTask;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.v2.app.job.impl.MapTaskImpl;

import cn.edu.xjtu.models.Trajectory;
import cn.edu.xjtu.utils.FileUtils;
import cn.edu.xjtu.utils.LogUtils;
import cn.edu.xjtu.utils.PrivacyConfig;
import cn.edu.xjtu.utils.PrivacyUtils;

/**
 * Created by YGZ on 2016/4/19.
 * My email : errrrorer@foxmail.com
 */
public class HadoopMain {
    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
        if (args.length<2){
            throw new IllegalArgumentException("Arguments: <input dir> <output dir> <config file>");
        }
        String cnfFile = "config/privacy.properties";
        if (args.length==3){
            cnfFile = args[2];
        }
        String interPath = "";
        Random random = new Random();
        String key = Integer.toString(random.nextInt());
        Configuration conf = new Configuration();
        PrivacyConfig.initialize(cnfFile);
        conf.set("cnfFile",cnfFile);
        Path interResultPath = new Path(interPath);
        FileSystem fs = interResultPath.getFileSystem(conf);
        if (fs.exists(interResultPath)){
            fs.delete(interResultPath);
            System.out.println(interResultPath.getName()+"存在，已删除！");
        }

        LogUtils.getInstance().append("------begin-------");
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        LogUtils.getInstance().append("Time:\t"+format.format(now));
        LogUtils.getInstance().append("输入目录：\t"+args[0]);
        LogUtils.getInstance().append("输出目录：\t"+args[1]);
        LogUtils.getInstance().append("配置文件：\t"+cnfFile);
        long time0=System.currentTimeMillis();


        Job job =Job.getInstance(conf,"Myhw");
        job.setJarByClass(HadoopMain.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,interResultPath);
        job.setInputFormatClass(FileNameAsKeyLineInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(DateTimeAsKeyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongLine.class);
        job.setPartitionerClass(LongLinePartition.class);
        job.setReducerClass(TimeSectionReducer.class);
        job.setNumReduceTasks(24/PrivacyConfig.getInstance().getHowManyHoursPerSection());
        job.waitForCompletion(true);



    }
}

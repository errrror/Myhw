package cn.edu.xjtu.Hadoop;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Created by YGZ on 2016/5/4.
 * My email : errrrorer@foxmail.com
 */
public class NoTabWriter extends RecordWriter<Text,Text>{
    private TaskAttemptContext job;
    private Map<Text,PrintWriter> writers=new HashMap();
    private Path outputPath;
    private String inputPathStr;
    private String outputPathStr;
    public NoTabWriter(TaskAttemptContext job,Path outputPath) {
        this.job=job;
        this.outputPath=outputPath;
        this.inputPathStr=job.getConfiguration().get("inputPath");
        this.outputPathStr=job.getConfiguration().get("outputPath");
    }

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
        if (!this.writers.containsKey(key)){
            Path path = new Path(key.toString().replace(inputPathStr, outputPathStr));
            FSDataOutputStream fileOut = path.getFileSystem(job.getConfiguration()).create(path);
            this.writers.put(key,new PrintWriter(fileOut));
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

    }
}

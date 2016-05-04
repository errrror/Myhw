package cn.edu.xjtu.Hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by ygz on 16/5/4.
 * My email : errrrorer@foxmail.com
 */
public class LineToFileMapper extends Mapper<Text,Text,FileNameAndLineNumber,Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        int lineNumber = Integer.parseInt(values[0]);
        FileNameAndLineNumber fileNameAndLineNumber = new FileNameAndLineNumber(key,new Text(String.valueOf(lineNumber)));
        context.write(fileNameAndLineNumber,value);
    }
}

package cn.edu.xjtu.Hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by ygz on 16/5/4.
 * My email : errrrorer@foxmail.com
 */
public class FileNamePartitioner extends Partitioner<FileNameAndLineNumber,Text>{

    @Override
    public int getPartition(FileNameAndLineNumber fileNameAndLineNumber, Text text, int numPartitions) {
        HashPartitioner<Text,Text> hashPartitioner = new HashPartitioner<Text, Text>();
        return hashPartitioner.getPartition(fileNameAndLineNumber.getFileName(),text,numPartitions);
    }
}

package cn.edu.xjtu.Hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ygz on 16/5/4.
 * My email : errrrorer@foxmail.com
 */
public class FileGroupingComparator extends WritableComparator {
    protected FileGroupingComparator() {
        super(FileNameAndLineNumber.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FileNameAndLineNumber fileNameAndLineNumber1=(FileNameAndLineNumber)a;
        FileNameAndLineNumber fileNameAndLineNumber2=(FileNameAndLineNumber)b;
        //分组时只比较fileName
        return fileNameAndLineNumber1.getFileName().compareTo(fileNameAndLineNumber2.getFileName());
    }
}

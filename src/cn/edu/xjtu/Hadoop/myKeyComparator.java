package cn.edu.xjtu.Hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ygz on 16/5/4.
 * My email : errrrorer@foxmail.com
 */
public class myKeyComparator extends WritableComparator{
    protected myKeyComparator() {
        super(FileNameAndLineNumber.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FileNameAndLineNumber fileNameAndLineNumber1 = (FileNameAndLineNumber)a;
        FileNameAndLineNumber fileNameAndLineNumber2 = (FileNameAndLineNumber)b;
        return fileNameAndLineNumber1.compareTo(fileNameAndLineNumber2);
    }
}

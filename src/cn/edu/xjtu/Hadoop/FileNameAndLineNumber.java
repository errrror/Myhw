package cn.edu.xjtu.Hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ygz on 16/5/4.
 * My email : errrrorer@foxmail.com
 */
public class FileNameAndLineNumber implements WritableComparable<FileNameAndLineNumber> {

    private Text fileName;
    private Text lineNumber;

    /**
     * 必须有默认的构造函数，这样mapreduce才能创建对象，然后通过readFields方法从序列化的数据流中读出数据进行赋值
     */
    public FileNameAndLineNumber(){
        this.fileName=new Text();
        this.lineNumber=new Text();
    }

    public FileNameAndLineNumber(Text FileName,Text lineNumber) {
        this.fileName=FileName;
        this.lineNumber=lineNumber;
    }
    @Override
    public int compareTo(FileNameAndLineNumber o) {
        int cmp = fileName.compareTo(o.fileName);
        if (cmp!=0) return cmp;
        int lineNum1 = Integer.parseInt(lineNumber.toString());
        int lineNum2 = Integer.parseInt(o.lineNumber.toString());
        return lineNum1-lineNum2;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        fileName.write(out);
        lineNumber.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        fileName.readFields(in);
        lineNumber.readFields(in);
    }

    public Text getFileName() {
        return fileName;
    }
}

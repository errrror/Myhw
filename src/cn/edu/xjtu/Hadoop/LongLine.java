package cn.edu.xjtu.Hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by YGZ on 2016/4/19.
 * My email : errrrorer@foxmail.com
 */
public class LongLine implements WritableComparable<LongLine> {
    private Text filename;
    private Text lineNumber;
    private Text line;

    public LongLine(){
        this.filename = new Text();
        this.line = new Text();
        this.lineNumber = new Text();
    }

    public LongLine(Text filename, Text lineNumber, Text line) {
        this.filename = filename;
        this.lineNumber = lineNumber;
        this.line = line;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        filename.write(out);
        lineNumber.write(out);
        line.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        filename.readFields(in);
        lineNumber.readFields(in);
        line.readFields(in);
    }

    @Override
    public int compareTo(LongLine o) {
        int cmp = filename.compareTo(o.getFilename());
        if (cmp != 0) return cmp;
        cmp = Integer.parseInt(lineNumber.toString())-Integer.parseInt(o.getLineNumber().toString());
        if (cmp != 0) return cmp;
        return line.compareTo(o.getLine());
    }

    public Text getFilename() {
        return filename;
    }

    public void setFilename(Text filename) {
        this.filename = filename;
    }

    public Text getLineNumber() {
        return lineNumber;
    }

    public void setLineNumber(Text lineNumber) {
        this.lineNumber = lineNumber;
    }

    public Text getLine() {
        return line;
    }

    public void setLine(Text line) {
        this.line = line;
    }
}

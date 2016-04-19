package cn.edu.xjtu.Hadoop;

import cn.edu.xjtu.utils.PrivacyConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by YGZ on 2016/4/19.
 * My email : errrrorer@foxmail.com
 */
public class LongLinePartition extends Partitioner<Text,LongLine> {
    @Override
    public int getPartition(Text text, LongLine longLine, int numPartitions) {
        String dateTime = text.toString();
        String time = dateTime.substring(10,12);
        int t = Integer.parseInt(time);
        int howManyHoursPerSection= PrivacyConfig.getInstance().getHowManyHoursPerSection();
        return ((t/howManyHoursPerSection)%numPartitions);
    }
}

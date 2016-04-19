package cn.edu.xjtu.Hadoop;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.edu.xjtu.models.DataPoint;
import cn.edu.xjtu.models.Location;
import cn.edu.xjtu.models.Trajectory;
import cn.edu.xjtu.utils.DataUtils;
import cn.edu.xjtu.utils.PrivacyConfig;
import cn.edu.xjtu.utils.PrivacyUtils;

/**
 * Created by YGZ on 2016/4/19.
 * My email : errrrorer@foxmail.com
 */
public class DateTimeAsKeyMapper extends Mapper<Text,Text,Text,LongLine> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        PrivacyConfig.initialize(conf.get("cnfFile"));
        int howManyHoursPerSection = PrivacyConfig.getInstance().getHowManyHoursPerSection();
        String lineNumAndcurrentLine = value.toString();
        String[] values = lineNumAndcurrentLine.split("\t");
        int lineNumber = Integer.parseInt(values[0]);
        StringBuilder linebuilder = new StringBuilder();
        for (int i = 1; i < values.length - 1; i++)
            linebuilder.append(values[i] + "\t");
        linebuilder.append(values[values.length - 1]);
        String currentLine = linebuilder.toString();
        if (lineNumber < 1)// 不读第一行
            currentLine = null;

        if (currentLine != null){
            String ts = currentLine.split("\t")[0];
            String date = ts.split(" ")[0];
            String time = ts.split(" ")[1].substring(0,2);
            time = cn.edu.xjtu.Hadoop.DateUtils.floorHour(time,howManyHoursPerSection);
            String dateTime = date + time;
            LongLine texts = new LongLine(key,new Text(String.valueOf(lineNumber)),new Text(currentLine));
            context.write(new Text(dateTime),texts);
        }
    }
}

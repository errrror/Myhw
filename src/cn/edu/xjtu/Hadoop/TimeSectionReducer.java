package cn.edu.xjtu.Hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.xjtu.models.DataPoint;
import cn.edu.xjtu.models.Location;
import cn.edu.xjtu.models.Trajectory;
import cn.edu.xjtu.utils.DataUtils;
import cn.edu.xjtu.utils.LogUtils;
import cn.edu.xjtu.utils.PrivacyConfig;
import cn.edu.xjtu.utils.PrivacyUtils;
import cn.edu.xjtu.utils.StringProtectUtils;
import cn.edu.xjtu.utils.TimeUtils;

/**
 * Created by YGZ on 2016/4/19.
 * My email : errrrorer@foxmail.com
 */


/**
 *
 * @author zhanchen
 * 输入key是日期和起始时间，value是包含文件名、行号、行的对象
 * 输出key是文件名，value是行号+"\t"+"行"
 * 为了解决不同的reduce写同一个文件时彼此覆盖的问题，每个reduce只写自己负责处理的那一部分，
 * 且在这个reduce中不管文件中的排序问题，在后续的job中，再根据行号进行排序
 *
 */
public class TimeSectionReducer extends Reducer<Text,LongLine,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<LongLine> values, Context context) throws IOException, InterruptedException {
        PrivacyConfig.initialize(context.getConfiguration().get("cnfFile"));
        LogUtils.getInstance().append("Task: "+context.getTaskAttemptID().toString());
        Iterator<LongLine> valueItor = values.iterator();
        List<DataPoint> dataPoints = new ArrayList<DataPoint>();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        while(valueItor.hasNext()){
            LongLine texts = valueItor.next();
            Text fileName = texts.getFilename();
            int lineNumber = Integer.parseInt(texts.getLineNumber().toString());
            String currenLine = texts.getLine().toString();
            if (currenLine!=null){
                String[] cols = currenLine.split("\t");
                Date date = null;
                try{
                    date = format.parse(cols[0]);
                }catch(ParseException e){
                    e.printStackTrace();
                }
                DataPoint dataPoint = new DataPoint(cols[1],new Location(Double.parseDouble(cols[10]),Double.parseDouble(cols[11])),TimeUtils.Date2TimeStamp(date),fileName.toString(),lineNumber);
                dataPoints.add(dataPoint);
            }
        }
        if (dataPoints.size()==0){
            return;
        }
        List<Trajectory> originTs = PrivacyUtils.getTrajectories(dataPoints);
        List<Trajectory> resultTrajectories = PrivacyUtils
                .protectPrivacy(originTs);

        dataPoints = new ArrayList<DataPoint>();
        for (Trajectory trajectory : resultTrajectories) {
            dataPoints.addAll(trajectory.getDataPoints());
        }

        Comparator<DataPoint> comparator = new Comparator<DataPoint>() {
            @Override
            public int compare(DataPoint dp1, DataPoint dp2) {
                if(dp1.getFileName().compareTo(dp2.getFileName()) < 0)
                    return -1;
                else if (dp1.getFileName().compareTo(dp2.getFileName()) > 0)
                    return 1;
                else {
                    if (dp1.getLineNumber() < dp2.getLineNumber())
                        return -1;
                    else if (dp1.getLineNumber() > dp2.getLineNumber())
                        return 1;
                    else
                        return 0;
                }
            }
        };
        Collections.sort(dataPoints,comparator);

        Set<List<DataPoint>> dataPointss = new HashSet<>();
        String currentFileName = dataPoints.get(0).getFileName();
        List<DataPoint> dataPoints_sameFile = new ArrayList<>();
        for (int i = 0; i < dataPoints.size(); i++) {
            DataPoint dp = dataPoints.get(i);
            if (!dp.getFileName().equals(currentFileName)) {
                dataPointss.add(dataPoints_sameFile);
                dataPoints_sameFile = new ArrayList<>();
                currentFileName = dp.getFileName();
            } else if (i == dataPoints.size() - 1) {
                dataPointss.add(dataPoints_sameFile);
            }
            dataPoints_sameFile.add(dp);
        }

        Map<String, Scanner> scanners = new HashMap<>();
        for (List<DataPoint> dps : dataPointss) {
            String fileName = dps.get(0).getFileName();
            Scanner scanner = null;
            if (scanners.get(fileName) == null) {
                Path path = new Path(fileName);
                FileSystem fs = path.getFileSystem(context.getConfiguration());
                FSDataInputStream in = fs.open(path);
                scanners.put(fileName, new Scanner(in));
            }
            scanner = scanners.get(fileName);
            try {
                int lineNumber = -1;
                int dpsIndex = 0;
                while (scanner.hasNextLine()) {
                    if (dpsIndex > dps.size() - 1)
                        break; // 数据点已读完，剩下的数据原样写入即可
                    String line = scanner.nextLine();
                    lineNumber++;
                    String[] attributes = line.split("\t");
                    DataPoint dataPoint = dps.get(dpsIndex);
                    if (dataPoint.getLineNumber() == lineNumber) {
                        dpsIndex++;
                        attributes[0] = format.format(TimeUtils.TimeStamp2Date(dataPoint.getTime()));
                        attributes[1] = dataPoint.getUserId();
                        attributes[10] = String.valueOf(dataPoint.getLocation()
                                .getLongitude());
                        attributes[11] = String.valueOf(dataPoint.getLocation()
                                .getLatitude());
                        line = String.join("\t", attributes);
                        line=String.valueOf(lineNumber)+"\t"+line;
                        context.write(new Text(fileName), new Text(line));
                    } else {// 针对未读取的无效数据
                        /**
                         * 未读取的数据不写入
                         * 将会在后续的job中写入
                         * 这样做的目的是每个reduce只写自己负责的那部分，避免与其他reduce产生写冲突
                         */
                        continue;
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                scanner.close();
            }
        }
    }
    }
}

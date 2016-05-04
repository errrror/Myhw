package cn.edu.xjtu.Hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.xjtu.models.DataPoint;
import cn.edu.xjtu.models.Location;
import cn.edu.xjtu.models.Trajectory;
import cn.edu.xjtu.utils.PrivacyConfig;
import cn.edu.xjtu.utils.PrivacyUtils;
import cn.edu.xjtu.utils.StringProtectUtils;

/**
 * Created by ygz on 16/5/4.
 * My email : errrrorer@foxmail.com
 */
public class myReducer extends Reducer<FileNameAndLineNumber, Text, Text, Text> {

    @Override
    protected void reduce(FileNameAndLineNumber key, Iterable<Text> values,
                          Reducer<FileNameAndLineNumber, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {

        Map<String, Scanner> scanners = new HashMap<>();
        String srcDir = context.getConfiguration().get("inputPath");
        String desDir = context.getConfiguration().get("outputPath");
        String keyHMAC=context.getConfiguration().get("key");
        HDFSUtils.copyAllFileStructureWithoutContent(srcDir, desDir);

        String fileName = key.getFileName().toString();
        Scanner scanner = null;
        if (scanners.get(fileName) == null) {
            Path path = new Path(fileName);
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            FSDataInputStream in = fs.open(path);
            scanners.put(fileName, new Scanner(in));
        }
        scanner = scanners.get(fileName);
        Iterator<Text> valueItor = values.iterator();
        List<Text> dps = new ArrayList<Text>();
        while (valueItor.hasNext()) {
            dps.add(new Text(valueItor.next()));
        }
        try {
            int lineNumber = -1;
            int dpsIndex = 0;
            while (scanner.hasNextLine()) {
                if (dpsIndex > dps.size() - 1)
                    break; // 数据点已读完，剩下的数据原样写入即可
                String line = scanner.nextLine();
                lineNumber++;
                String[] attributes = line.split("\t");
                Text dataPoint = dps.get(dpsIndex);
                if (Integer.parseInt(dataPoint.toString().split("\t")[0]) == lineNumber) {
                    dpsIndex++;
                    String[] tokens = dataPoint.toString().split("\t");
                    StringBuilder newLine = new StringBuilder();
                    for (int i = 1; i < tokens.length - 1; i++) {
                        if (i == 2) {
                            newLine.append(StringProtectUtils.encryptHMAC(tokens[i], keyHMAC)
                                    + "\t");
                        } else {
                            newLine.append(tokens[i] + "\t");
                        }
                    }
                    newLine.append(tokens[tokens.length - 1]);
                    line = newLine.toString();
                } else {// 针对未读取的无效数据
                    String userId = attributes[1];
                    String hashedUserId = StringProtectUtils.encryptHMAC(userId, keyHMAC);
                    if (lineNumber == 0)
                        hashedUserId = userId;// 第一行是列名，不需要hash
                    attributes[1] = hashedUserId;
                    line = String.join("\t", attributes);
                }
                context.write(new Text(fileName), new Text(line));
            }
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                lineNumber++;
                String[] attributes = line.split("\t");
                String userId = attributes[1];
                String hashedUserId = StringProtectUtils.encryptHMAC(userId, keyHMAC);
                if (lineNumber == 0)
                    hashedUserId = userId;// 第一行是列名，不需要hash
                attributes[1] = hashedUserId;
                line = String.join("\t", attributes);
                context.write(new Text(fileName), new Text(line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            scanner.close();
        }
    }
}

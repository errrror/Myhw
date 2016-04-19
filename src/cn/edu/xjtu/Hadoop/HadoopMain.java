package cn.edu.xjtu.Hadoop;

import java.io.IOException;

/**
 * Created by YGZ on 2016/4/19.
 * My email : errrrorer@foxmail.com
 */
public class HadoopMain {
    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
        if (args.length<2){
            throw new IllegalArgumentException("Arguments: <input dir> <output dir> <config file>");
        }
        String cnfFile = "config/privacy.properties";
        if (args.length==3){
            cnfFile = args[2];
        }


    }
}

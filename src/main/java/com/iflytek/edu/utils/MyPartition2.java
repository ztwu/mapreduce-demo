package com.iflytek.edu.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/29
 * Time: 15:49
 * Description
 */

public class MyPartition2 extends Partitioner<Text, Text> {

    public int getPartition(Text key, Text value, int i) {

        String schoolId = key.toString();
        if ("0001".equals(schoolId)){
            return 1;
        }else if("0002".equals(schoolId)){
            return 1;
        }else if("0003".equals(schoolId)){
            return 0;
        }
        return 0;
    }
}

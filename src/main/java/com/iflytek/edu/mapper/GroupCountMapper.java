package com.iflytek.edu.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2018/5/21
 * Time: 8:34
 * Description
 */

public class GroupCountMapper extends Mapper<Object,Text,Text,LongWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("map端分组聚合输入key值 ："+key);
        System.out.println("map端分组聚合输入value值 ："+value);

        String[] values = value.toString().split("\\t");
        System.out.println(values.length);

        String schoolId = values[3];
        String schoolName = values[9];
        String eventId = values[6];

        //可以针对null的count字段过滤掉，实现count(null)不计入计算
        context.write(new Text(schoolId+"\\t"+schoolName+"\\t"+eventId),new LongWritable(1));

    }
}

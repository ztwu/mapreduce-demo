package com.iflytek.edu.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2018/5/21
 * Time: 8:45
 * Description
 */

public class GroupCountReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

    Map<String,Long> map = new HashMap<String,Long>();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("reduce端分组聚合输入key值 ："+key);
        long count = 0;

        //count(1)计算
        for(LongWritable item:values){
            count += item.get() ;
            System.out.println("reduce端分组聚合输入value值 ："+item.get());
        }
        context.write(key,new LongWritable(count));
    }
}

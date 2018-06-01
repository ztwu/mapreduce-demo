package com.iflytek.edu.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2018/5/21
 * Time: 8:34
 * Description
 */

public class GroupCountMapper extends Mapper<Object,Text,Text,LongWritable> {

    Map<String,String> cacheMaps = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        if(cacheFiles!=null&&cacheFiles.length>0){
            String path = cacheFiles[0].getPath();
            System.out.println("分布式缓存的数据路径："+path);

            //读取hdfs分布式文件
            Path p = new Path(path);
            Configuration configuration = context.getConfiguration();
            FileSystem fs = p.getFileSystem(configuration);
            FSDataInputStream fis = fs.open(p);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis,"UTF-8"));
            String line = null;
            while((line = br.readLine())!=null){
                System.out.println("GroupCountMapper map段机器上获取分布式缓存的数据："+line);
                String[] temp = line.split(",");
                cacheMaps.put(temp[0],temp[1]);
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("map端分组聚合输入key值 ："+key);
        System.out.println("map端分组聚合输入value值 ："+value);

        String[] values = value.toString().split("\\t");
        System.out.println(values.length);

        String cityId = values[1];
        String schoolId = values[3];
        String schoolName = values[9];
        String eventId = values[6];

        Set<String> sets =  cacheMaps.keySet();
        String cityName = null;
        for(String item : sets){
            System.out.println("分布式共享市的id : "+item);
            System.out.println("mapreduce的map获取市的名称 : "+cityName);
            if(item.equals(cityId)){
                cityName = cacheMaps.get(item);
                //可以针对null的count字段过滤掉，实现count(null)不计入计算
                //放在for循环里面就是join,没有关联到的直接过滤
                context.write(new Text(cityId+"\t"+cityName+"\t"+cityId+schoolId+"\t"+schoolName+"\t"+eventId),new LongWritable(1));
                break;
            }
        }
    }
}

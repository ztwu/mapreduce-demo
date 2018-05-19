package com.iflytek.edu.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.File;
import java.io.IOException;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/25
 * Time: 20:33
 * Description
 */

//相应操作一个文件inputsplit的map,可以自己定义对应的key
public class MyRecordReader extends RecordReader {

    //起始位置(相对整个分片而言)
    private long start;
    //结束位置(相对整个分片而言)
    private long end;
    //当前位置
    private long pos;
    //文件输入流
    private FSDataInputStream fin = null;
    //key、value
    private LongWritable key = null;
    private Text value = null;
    //定义行阅读器(hadoop.util包下的类)
    private LineReader reader = null;

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //获取分片
        FileSplit fileSplit = (FileSplit) inputSplit;
        //获取起始位置
        start = fileSplit.getStart();
        //获取结束位置
        end = start + fileSplit.getLength();
        //创建配置
        Configuration conf = taskAttemptContext.getConfiguration();
        //获取文件路径
        Path path = fileSplit.getPath();
        //根据路径获取文件系统
        FileSystem fileSystem = path.getFileSystem(conf);
        //打开文件输入流
        fin = fileSystem.open(path);
        //找到开始位置开始读取
        fin.seek(start);
        //创建阅读器
        reader = new LineReader(fin);
        //将当期位置置为1
        pos = 1;
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null){
            key = new LongWritable();
        }
        //以行号作为每个split文件的每行记录的key
        key.set(pos);
        if (value == null){
            value = new Text();
        }

        //读取每行记录的数据输出流
        if (reader.readLine(value) == 0){
            return false;
        }

        System.out.println("------------------读取inputsply文件 : "+value.toString());
        //自定json文件解析
        String temp = value.toString();
        try {
            JSONObject obj = JSON.parseObject(temp);
            if(obj!=null){
                String userId = obj.getString("userId");
                String event = obj.getString("event");
                value.set(userId+"\t"+event);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        pos ++;

        return true;
    }

    public Object getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public Object getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {
        fin.close();
    }
}

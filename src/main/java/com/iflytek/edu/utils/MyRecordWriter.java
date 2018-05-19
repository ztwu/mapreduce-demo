package com.iflytek.edu.utils;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/25
 * Time: 20:48
 * Description
 */

public class MyRecordWriter<K,V> extends RecordWriter<K, V> {

    FSDataOutputStream fsDataOutputStream;

    // 具体的写数据的方法
    public void write(K key, V value) throws IOException, InterruptedException {
        System.out.println("自定数据输出");
        System.out.println("key:"+key.toString());
        System.out.println("value:"+value.toString().trim());

//        fsDataOutputStream.writeUTF(value.toString().trim()+"\n");

        //输出为txt文件
        fsDataOutputStream.writeChars(value.toString()+"\n");

    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fsDataOutputStream.close();
    }

    //传入writer需要的分布式文件输入流
    public MyRecordWriter(FSDataOutputStream fsDataOutputStream) {
        this.fsDataOutputStream = fsDataOutputStream;
    }
}

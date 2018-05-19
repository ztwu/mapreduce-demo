package com.iflytek.edu.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/25
 * Time: 20:31
 * Description
 */

public class MyInputFormat extends FileInputFormat<Text, Text> {

    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MyRecordReader();
    }

    /**
     * 为了使得切分数据的时候行号不发生错乱
     * 这里设置为不进行切分
     */
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

}

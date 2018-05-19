package com.iflytek.edu.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/25
 * Time: 20:47
 * Description
 */

public class MyOutputFormat extends FileOutputFormat<Text, Text> {

    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf = taskAttemptContext.getConfiguration();
        FileSystem fs = FileSystem.newInstance(conf);

        //创建输出目标文件的输出流
        Path file = getDefaultWorkFile(taskAttemptContext,".txt");
        FSDataOutputStream fsDataOutputStream = fs.create(file);

        return new MyRecordWriter<Text,Text>(fsDataOutputStream);
    }

    /**
     * get task output path
     * @param conf
     * @return
     * @throws IOException
     */
    private Path getTaskOutputPath(TaskAttemptContext conf) throws IOException {
        Path workPath = null;
        OutputCommitter committer = super.getOutputCommitter(conf);
        if (committer instanceof FileOutputCommitter) {
            workPath = ((FileOutputCommitter) committer).getWorkPath();
        } else {
            Path outputPath = super.getOutputPath(conf);
            if (outputPath == null) {
                throw new IOException("Undefined job output-path");
            }
            workPath = outputPath;
        }
        System.out.println("获取输出路径;"+workPath);
        return workPath;
    }

}

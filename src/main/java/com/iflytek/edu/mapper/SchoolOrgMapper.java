package com.iflytek.edu.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.io.IOException;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/8
 * Time: 13:31
 * Description
 */

public class SchoolOrgMapper extends Mapper<Object, SimpleGroup , Text, Text> {

    String fileCode = "";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String path = fileSplit.getPath().getName();
        fileCode = path.split("_")[0];
        System.out.println("SchoolOrgMapper : " + fileCode);
    }

    @Override
    protected void map(Object key, SimpleGroup value, Context context) throws IOException, InterruptedException {

        String schoolId = value.getString("school_id",0);

        System.out.println("SchoolOrgMapper : "+schoolId);
        System.out.println("SchoolOrgMapper : "+value.toString());

        context.write(new Text(schoolId), new Text(value.toString()));

    }

}

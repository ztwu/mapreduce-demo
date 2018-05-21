package com.iflytek.edu.main;

import com.iflytek.edu.bean.DwsUcUserOrganizationBean;
import com.iflytek.edu.mapper.DwsLogUserActiveMapper;
import com.iflytek.edu.mapper.DwsUcUserOrganizationMapper;
import com.iflytek.edu.mapper.GroupCountMapper;
import com.iflytek.edu.mapper.SchoolOrgMapper;
import com.iflytek.edu.reducer.GroupCountReducer;
import com.iflytek.edu.reducer.ZhktUserBaseReducer;
import com.iflytek.edu.reducer.ZhktUserInfoReducer;
import com.iflytek.edu.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageTypeParser;

import java.net.URI;
import java.util.Set;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/8/14
 * Time: 19:22
 * Description
 */

public class ZhktUserBaseRunner {

    public static void main(String[] args) throws Exception {
        String inputpath1 = "data/project/edu_edcc/ztwu2/temp/mapreduce-data/dws_uc_user_organization";
        String inputpath2 = "data/project/edu_edcc/ztwu2/temp/mapreduce-data/dws_log_user_active";
        String inputpath3 = "data/project/edu_edcc/ztwu2/temp/mapreduce-data/dw_school_org";

        String midtemppath1 = "data/project/edu_edcc/ztwu2/temp/mapreduce-mid-temp/temp1";
        String midtemppath2 = "data/project/edu_edcc/ztwu2/temp/mapreduce-mid-temp/temp2";

        String outputpath = "data/project/edu_edcc/ztwu2/temp/mapreduce-result";

        String cacheFilePath = "data/project/edu_edcc/ztwu2/temp/mapreduce-data/distributecache/data.txt";

        //获得Configuration配置 Configuration: core-default.xml, core-site.xml
        Configuration conf = new Configuration();

        Util.cleanInputpath(conf, midtemppath1, midtemppath2, outputpath);

        String writeSchema = "message example {\n" +
                "required binary province_id;\n" +
                "required binary city_id;\n" +
                "required binary district_id;\n" +
                "required binary school_id;\n" +
                "required binary tableA.user_id;\n" +
                "required binary tableB.user_id;\n" +
                "required binary appkey;\n" +
                "required binary flag;\n" +
                "}";
        conf.set("parquet.example.schema",writeSchema);

        //job1
        Job job1 = Job.getInstance(conf,"ztwu2-job-step1");

        //分布式缓存数据
        //指定需要缓存一个文件到所有的maptask运行节点工作目录
//      job1.addArchiveToClassPath(archive);缓存jar包到task运行节点的classpath中
//      job1.addCacheArchive(uri);缓存压缩包到task运行节点的工作目录
//      job1.addFileToClassPath(file);//缓存普通文件到task运行节点的classpath中

//      URI,地址中涉及了特殊字符会报错
        job1.addCacheFile(new URI(cacheFilePath));

        ///设置Job属性
        job1.setJarByClass(ZhktUserBaseRunner.class);
        job1.setMapperClass(DwsUcUserOrganizationMapper.class);
        job1.setReducerClass(ZhktUserBaseReducer.class);

        //设置Map自定义的key
        //自定义排序
        job1.setMapOutputKeyClass(MyKeyPair.class);
        //设置Map自定义的value
        job1.setMapOutputValueClass(DwsUcUserOrganizationBean.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        //自定义分区
        //一个分区可以对应一个或多个分组集合
        job1.setPartitionerClass(MyPartition.class);
        //如何确认reduce的数量
        // 参数1： totalInputFileSize     job的所有输入的总的字节数
        //参数2： bytesPerReducer     每个reduce的数据量，由hive.exec.reducers.bytes.per.reducer参数指定，当前版本默认是256MB
        //参数3： maxReducers          一个maprduce作业所允许的最大的reduce数量，由参数hive.exec.reducers.max指定，默认是1099
        //min(max(totalInputFileSize/bytesPerReducer,1),maxReducers)来决定的
        job1.setNumReduceTasks(2);

        //Key排序之后，再去分组
        ////Key排序的规则：
//        1.如果调用jobconf的setOutputKeyComparatorClass()设置mapred.output.key.comparator.class
//        2.否则，使用key已经登记的comparator
//        3.否则，实现接口WritableComparable的compareTo()函数来操作
        job1.setSortComparatorClass(MySortComparator.class);

        //自定义分组
//        1.如果调用jobconf的setGroupingComparatorClass设置
//        2.否则，setOutputKeyComparatorClass()设置
//        3.否则，使用key已经登记的comparator
        job1.setGroupingComparatorClass(MyGroupingComparator.class);

        //传入input path
        //txt文件
//        FileInputFormat.setInputPaths(job1, new Path(inputpath1));

        //parquet文件
//        job1.setInputFormatClass(ParquetInputFormat.class);
//        ParquetInputFormat.setInputPaths(job1, new Path(inputpath1));
//        ParquetInputFormat.setReadSupportClass(job1, GroupReadSupport.class);

        //传入input path
        //RecordReader表示以怎样的方式从分片中读取一条记录，每读取一条记录都会调用RecordReader类，
        // 系统默认的RecordReader是LineRecordReader，它是TextInputFormat对应的RecordReader；
        // 而SequenceFileInputFormat对应的RecordReader是SequenceFileRecordReader。
        // LineRecordReader是每行的偏移量作为读入map的key，每行的内容作为读入map的value。
        // 很多时候hadoop内置的RecordReader并不能满足我们的需求，
        // 比如我们在读取记录的时候，希望Map读入的Key值不是偏移量而是行号或者是文件名，
        // 这时候就需要我们自定义RecordReader。
        MultipleInputs.addInputPath(job1,new Path(inputpath1),ParquetInputFormat.class, DwsUcUserOrganizationMapper.class);
        ParquetInputFormat.setReadSupportClass(job1, GroupReadSupport.class);
        //自定义文件inputformat
        MultipleInputs.addInputPath(job1,new Path(inputpath2), MyInputFormat.class, DwsLogUserActiveMapper.class);
        //传入output path
        job1.setOutputFormatClass(ParquetOutputFormat.class);
        ParquetOutputFormat.setOutputPath(job1, new Path(midtemppath1));
        ParquetOutputFormat.setWriteSupportClass(job1, GroupWriteSupport.class);

        //job2
        Job job2 = Job.getInstance(conf, "ztwu2-job-step2");

        job2.setJarByClass(ZhktUserBaseRunner.class);
        job2.setMapperClass(SchoolOrgMapper.class);
        job2.setReducerClass(ZhktUserInfoReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setPartitionerClass(MyPartition2.class);
        job2.setNumReduceTasks(2);

        ////传入input path
        //自定义文件输入类
        job2.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.addInputPath(job2, new Path(inputpath3));
        ParquetInputFormat.addInputPath(job2, new Path(midtemppath1));
        ParquetInputFormat.setReadSupportClass(job2,GroupReadSupport.class);
        //传入output path
        //自定义output
        job2.setOutputFormatClass(MyOutputFormat.class);
        MyOutputFormat.setOutputPath(job2, new Path(midtemppath2));

        //job3
        Job job3 = Job.getInstance(conf,"ztwu2-job-step3");
        job3.setJarByClass(ZhktUserBaseRunner.class);
        job3.setMapperClass(GroupCountMapper.class);
        job3.setReducerClass(GroupCountReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setOutputValueClass(LongWritable.class);

        TextInputFormat.addInputPath(job3,new Path(midtemppath2));
//        job3.setOutputValueClass(ParquetInputFormat.class);
//        ParquetOutputFormat.setOutputPath(job3,new Path(outputpath));
        TextOutputFormat.setOutputPath(job3,new Path(outputpath));
        //聚合函数操作强制在一个机器节点进行计算
        job3.setNumReduceTasks(1);
        job3.setCombinerClass(GroupCountReducer.class);

        //受控制的job
        ControlledJob cjob1 = new ControlledJob(conf);
        ControlledJob cjob2 = new ControlledJob(conf);
        ControlledJob cjob3 = new ControlledJob(conf);

        cjob1.setJob(job1);
        cjob2.setJob(job2);
        cjob3.setJob(job3);

        cjob2.addDependingJob(cjob1);
        cjob3.addDependingJob(cjob2);

        JobControl jc = new JobControl("job start");
        jc.addJob(cjob1);
        jc.addJob(cjob2);
        jc.addJob(cjob3);

        Thread jcThread = new Thread(jc);
        jcThread.start();

        boolean stopMe = true;

        while(stopMe){
            if(jc.allFinished()){
                System.out.println(jc.getSuccessfulJobList());
                jc.stop();
                Thread.yield();
                stopMe = false;
            }
            if(jc.getFailedJobList().size() > 0){
                System.out.println(jc.getFailedJobList());
                jc.stop();
                stopMe = false;
            }
        }
    }

}


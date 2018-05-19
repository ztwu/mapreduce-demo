package com.iflytek.edu.mapper;

import com.iflytek.edu.bean.DwsUcUserOrganizationBean;
import com.iflytek.edu.utils.MyKeyPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.LineReader;

import java.io.*;
import java.net.URI;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/7
 * Time: 17:21
 * Description
 */

public class DwsLogUserActiveMapper extends Mapper<Object, Text, MyKeyPair, DwsUcUserOrganizationBean> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        //所有字节输入流的超类，一般使用它的子类：FileInputStream等，它能输出字节流；
//        InputStream io = new FileInputStream("data.txt");
//        //是字节流与字符流之间的桥梁，能将字节流输出为字符流，并且能为字节流指定字符集，可输出一个个的字符；
//        InputStreamReader fis = new InputStreamReader(io);
//        // 提供通用的缓冲方式文本读取，readLine读取一个文本行， 从字符输入流中读取文本，缓冲各个字符，从而提供字符、数组和行的高效读取。
//        BufferedReader br = new BufferedReader(fis);
//
//        String line = null;
//        while((line = br.readLine())!=null){
//            System.out.println("分布式缓存的数据："+line);
//        }

        //这里的getLocalCacheFiles方法也被注解为过时了，只能使用context.getCacheFiles方法，
        // 和getLocalCacheFiles不同的是，getCacheFiles得到的路径是HDFS上的文件路径，
        // 如果使用这个方法，那么程序中读取的就不再试缓存在各个节点上的数据了，
        // 相当于共同访问HDFS上的同一个文件。
        //可以直接通过符号连接来跳过getLocalCacheFiles获得本地的文件。
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
                System.out.println("分布式缓存的数据："+line);
            }

            //读取节点本地文件
//            File file = new File(path);
//            FileReader fr = new FileReader(file);
//            BufferedReader br = new BufferedReader(fr);
//            String line = null;
//            while((line = br.readLine())!=null){
//                System.out.println("分布式缓存的数据："+line);
//            }

        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        String userId = fields[0];
        String appKey = fields[1];

        //"\t""\n"换行符一个字节
        System.out.println("DwsLogUserActiveMapper : key-"+ key + " : value-" + value.toString().length());
        System.out.println("DwsLogUserActiveMapper : "+ userId+" : "+appKey);

        context.write(new MyKeyPair(userId),new DwsUcUserOrganizationBean("bg-1","bg-1","bg-1","bg-1",userId,appKey,"DwsLogUserActive"));

    }
}

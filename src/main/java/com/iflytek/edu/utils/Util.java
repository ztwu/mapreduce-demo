package com.iflytek.edu.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/8
 * Time: 14:14
 * Description
 */

public class Util {

    public static void cleanInputpath(Configuration conf, String ...args) throws IOException {
        // 判断output文件夹是否存在，如果存在则删除
        if(args!=null&&args.length>0){
            for(String inputpath : args){
                Path path = new Path(inputpath);// 取第1个表示输出目录参数（第0个参数是输入目录）
                FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
                if (fileSystem.exists(path)) {
                    fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
                }
            }
        }
    }

}

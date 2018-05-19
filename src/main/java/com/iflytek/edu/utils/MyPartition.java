package com.iflytek.edu.utils;

import com.iflytek.edu.bean.DwsUcUserOrganizationBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.security.KeyPair;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/28
 * Time: 10:48
 * Description
 */

//自定义分区的目的是在分好了组之后，将不同的分组创建不同的reduce task任务
//Key的第一次排序,MyKeyPair
//Hadoop本身Key的数据类型的排序逻辑其实就是依赖于Hadoop本身的继承与WritableComparable<T>的基本数据类型和其他类型(相关类型可参考《Hadoop权威指南》第二版的90页)的compareTo方法的定义。
//Key排序的规则：
//        1.如果调用jobconf的setOutputKeyComparatorClass()设置mapred.output.key.comparator.class
//        2.否则，使用key已经登记的comparator
//        3.否则，实现接口WritableComparable的compareTo()函数来操作
public class MyPartition extends Partitioner<MyKeyPair, DwsUcUserOrganizationBean> {

    public int getPartition(MyKeyPair key, DwsUcUserOrganizationBean value, int num) {
        System.out.println("------------------------------------------------当前默认的分区数--："+num);

        String userId = key.getUserId();

        System.out.println("------------------------------------------------当前默认的分区数 key --："+key);

        if("u001".equals(userId)) {
            return 0;
        }else if("u002".equals(userId)) {
            return 0;
        }else if("u003".equals(userId)) {
            return 1;
        }else {
            return 1;
        }
    }

}

package com.iflytek.edu.utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/28
 * Time: 16:07
 * Description
 */

//Key排序
//在map和reduce阶段均有作用
//setOutputKeyComparatorClass(Class<? extends RawComparator> theClass)
//hadoop0.20.0以后的函数为setSortComparatorClass
public class MySortComparator extends WritableComparator{

    public MySortComparator(){
        super(MyKeyPair.class, true);
    }

    @Override
    //java 两种比较器
    //Collections.sort( personList , new PersonComparator() ).
    public int compare(WritableComparable a, WritableComparable b) {
        MyKeyPair o1 = (MyKeyPair) a;
        MyKeyPair o2 = (MyKeyPair) b;
        System.out.println("Sort == Comparator中的compare()方法是基于对象的比较 : "+o1.getUserId()+"<==>"+o2.getUserId());
        System.out.println("Sort == Comparator中的compare()方法是基于对象的比较 : "+(o2.getUserId().hashCode() - o1.getUserId().hashCode()));
        return o2.getUserId().hashCode() - o1.getUserId().hashCode();
    }

//    @Override
//    //基于字节排序，不需要序列化和反序列化
//    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//        System.out.println("Sort == Comparator中的compare()方法是基于字节的比较");
//        return WritableComparator.compareBytes(b1,s1,8, b2,s2,8);
//    }
}
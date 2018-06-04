package com.iflytek.edu.utils;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

import java.util.Arrays;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/28
 * Time: 15:38
 * Description
 */
//reduce，接受到key是一个自定义key，那么每个对象都不一样,需要自己处理这个MyKey的排序

//主要是在reduce接受数据阶段作用

//setOutputValueGroupingComparator(Class<? extends RawComparator> theClass）
//hadoop0.20.0以后的函数为setGroupingComparatorClass
//设置哪些value进入哪些key的迭代器中
public class MyGroupingComparator implements RawComparator<MyKeyPair> {

    /*
     * @param b1 表示第一个参与比较的字节数组
     *
     * @param s1 表示第一个参与比较的字节数组的起始位置
     *
     * @param l1 表示第一个参与比较的字节数组的偏移量
     *
     * @param b2 表示第二个参与比较的字节数组
     *
     * @param s2 表示第二个参与比较的字节数组的起始位置
     *
     * @param l2 表示第二个参与比较的字节数组的偏移量
     */
    ////基于字节排序，不需要序列化和反序列化
    //比较的时候，从开始的位置(s1和s2)，并且长度是(l1和l2)
    //允许实现它来在字节流的层面比较，而不用反序列化为对象，因此，可以避免过多的对象的创建。
    public int compare(byte[] b1, int s1, int i1,
                       byte[] b2, int s2, int i2) {
        System.out.println("Group == RawComparator中的compare()方法是基于字节的比较");
        System.out.println("Group b1 == "+ Arrays.toString(b1)+"="+s1+"="+i1);
        System.out.println("Group b2 == "+Arrays.toString(b1)+"="+s2+"="+i2);
        return WritableComparator.compareBytes(b1,s1,i1,
                b2,s2,i2);
    }

    //java 两种比较器
    //Collections.sort( Collections , new Comparator() ).
    public int compare(MyKeyPair o1, MyKeyPair o2) {
        System.out.println("Group == Comparator中的compare()方法是基于对象的比较 : "+o1.getUserId()+"<==>"+o2.getUserId());
        System.out.println("Group == Comparator中的compare()方法是基于对象的比较 : "+(o2.getUserId().hashCode() - o1.getUserId().hashCode()));
        return o2.getUserId().hashCode() - o1.getUserId().hashCode();
    }

}

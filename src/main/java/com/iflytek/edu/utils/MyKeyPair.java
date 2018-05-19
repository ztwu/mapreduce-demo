package com.iflytek.edu.utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/28
 * Time: 10:53
 * Description
 */

public class MyKeyPair implements WritableComparable<MyKeyPair> {

    private String userId;

    public MyKeyPair() {

    }

    public MyKeyPair(String userId) {
        this.userId = userId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    //java 两种比较器
    //Comparable 定义在 类的内部:
    public int compareTo(MyKeyPair o) {
        int first = this.getUserId().hashCode();
        int last = o.getUserId().hashCode();
        System.out.println("自定义key数据类型的比较 : "+this.getUserId()+"<==>"+o.getUserId());
        System.out.println("自定义key开始排序。。。" + (last-first));
        return (last - first);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(userId);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.userId = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return userId;
    }

    @Override
    public int hashCode() {
        return userId.hashCode();
    }

}

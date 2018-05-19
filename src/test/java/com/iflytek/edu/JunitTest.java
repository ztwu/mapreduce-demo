package com.iflytek.edu;

import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/25
 * Time: 19:56
 * Description
 */

public class JunitTest {

    @Test
    public void test1(){
        File file1 = new File("../../../data");
        File file2 = new File("data/inputpath/test.txt");
        System.out.println(file2.exists());
    }

    @Test
    public void test2() {
        List<Integer> A = new ArrayList<Integer>();
        List<Integer> B = new ArrayList<Integer>();
        A.add(1);
        A.add(1);
        A.add(1);
        A.add(1);
        A.add(1);
        A.add(1);
//        B.add(1);
//        B.add(2);
//        B.add(3);

        System.out.println("join");
        for(int a : A){
            for(int b : B){
                System.out.println(a+":"+b);
            }
        }

        System.out.println("left join");

        for(int a : A){
            if(B.size()>0){
                for(int b : B){
                    System.out.println(a+":"+b);
                }
            }else {
                System.out.println(a+":"+"null");
            }
        }

        System.out.println("right join");

        for(int b : B){
            for(int a : A){
                System.out.println(a+":"+b);
            }
        }

        String s1 = "u001";
        String s2 = "u002";
        String s3 = "u003";

        System.out.println(s1.hashCode() - s2.hashCode());

    }
}

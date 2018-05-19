package com.iflytek.edu.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/8
 * Time: 13:47
 * Description
 */

public class ZhktUserInfoReducer extends Reducer<Text, Text, Text, Text> {

    List<ArrayList<Text>> tableA = new ArrayList<ArrayList<Text>>();
    List<ArrayList<Text>> tableB = new ArrayList<ArrayList<Text>>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        tableA.clear();
        tableB.clear();

        System.out.println("数据输出：");
        for(Text item : values){

            String data = item.toString();
            String[] fields = data.split("\n");
            ArrayList<Text> listA = new ArrayList<Text>();
            ArrayList<Text> listB = new ArrayList<Text>();

            String flag = "";
            for(String field : fields){
                String[] f = field.split(":");
                if("flag".equals(f[0])&&"step1".equals(f[1].trim())){
                    flag = "A";
                }else {
                    flag = "B";
                }
            }
            for(String field : fields){
                String[] f = field.split(":");
                if("A".equals(flag)){
                    listA.add(new Text(f[1].trim()));
                }else if("B".equals(flag)) {
                    listB.add(new Text(f[1].trim()));
                }
            }
            if("A".equals(flag)){
                tableA.add(listA);
            }else if("B".equals(flag)) {
                tableB.add(listB);
            }
        }

        System.out.println("ZhktUserInfoReducer : "+key.toString());
        System.out.println("ZhktUserInfoReducer : "+tableB.size());

        //left join
        for(ArrayList itemA : tableA){
            String left = StringUtils.join(itemA,"\t");
//            System.out.println("ZhktUserInfoReducer : "+left);
            if(tableB.size()>0){
                for(ArrayList itemB : tableB){
                    String right = StringUtils.join(itemB,"\t");
                    System.out.println("ZhktUserInfoReducer : "+right);
                    context.write(key, new Text(left+"\t"+right));
                }
            }else {
                context.write(key,new Text(left+"\t"+"null"));
            }
        }

    }
}

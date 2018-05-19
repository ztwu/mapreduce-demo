package com.iflytek.edu.reducer;

import com.iflytek.edu.bean.DwsUcUserOrganizationBean;
import com.iflytek.edu.utils.MyKeyPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/6
 * Time: 10:45
 * Description
 */

public class ZhktUserBaseReducer extends Reducer<MyKeyPair, DwsUcUserOrganizationBean, Void, Group> {

    ArrayList<String> tableA = new ArrayList<String>();
    ArrayList<String> tableB = new ArrayList<String>();
    ArrayList<Group> result = new ArrayList<Group>();

    private SimpleGroupFactory factory;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(context.getConfiguration()));
    }

    @Override
    protected void reduce(MyKeyPair key, Iterable<DwsUcUserOrganizationBean> values, Context context)
            throws IOException, InterruptedException {

       //仅有一个datanode接口去执行reduce任务，任务同步执行
        tableA.clear();
       tableB.clear();
       result.clear();

       for(DwsUcUserOrganizationBean item : values){
           String flag = item.getFlag();
           String provinceId = item.getProvinceId();
           String cityId = item.getCityId();
           String districtId = item.getDistrictId();
           String schoolId = item.getSchoolId();
           String userId = item.getUserId();
           String appKey = item.getAppKey();

          StringBuffer sb = new StringBuffer();

           if("DwsLogUserActive".equals(flag)){
               sb.append(userId);
               sb.append("#");
               sb.append(appKey);
               tableB.add(sb.toString());
           }else if("DwsUcUserOrganization".equals(flag)){
               sb.append(provinceId);
               sb.append("#");
               sb.append(cityId);
               sb.append("#");
               sb.append(districtId);
               sb.append("#");
               sb.append(schoolId);
               sb.append("#");
               sb.append(userId);
               tableA.add(sb.toString());
           }
       }

        // left join
        for(String itemA : tableA){
            String[] tableArrary = itemA.split("#");
            if(tableB.size()>0){
                for(String itemB : tableB) {
                    Group data = factory.newGroup();
                    String[] tableArraryB = itemB.split("#");
                    data.append("province_id",tableArrary[0]);
                    data.append("city_id",tableArrary[1]);
                    data.append("district_id",tableArrary[2]);
                    data.append("school_id",tableArrary[3]);
                    data.append("tableA.user_id",tableArrary[4]);
                    data.append("tableB.user_id",tableArraryB[0]);
                    data.append("appkey",tableArraryB[1]);
                    data.append("flag","step1");
                    result.add(data);
                }
            }else {
                Group data = factory.newGroup();
                data.append("province_id",tableArrary[0]);
                data.append("city_id",tableArrary[1]);
                data.append("district_id",tableArrary[2]);
                data.append("school_id",tableArrary[3]);
                data.append("tableA.user_id",tableArrary[4]);
                data.append("tableB.user_id","null");
                data.append("appkey","null");
                data.append("flag","step1");
                result.add(data);
            }

            System.out.println("ZhktUserBaseReducer : ----------------key---------------------- "+key.getUserId());
            for(Group group : result) {
                System.out.println("ZhktUserBaseReducer : "+group);
                context.write(null, group);
            }
        }
    }
}

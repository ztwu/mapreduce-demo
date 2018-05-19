package com.iflytek.edu.mapper;

import com.iflytek.edu.bean.DwsUcUserOrganizationBean;
import com.iflytek.edu.utils.MyKeyPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/6
 * Time: 10:41
 * Description
 */

public class DwsUcUserOrganizationMapper extends Mapper<Object, SimpleGroup, MyKeyPair, DwsUcUserOrganizationBean> {

    String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String path = getFilePath(context);
        fileName = path.split("_")[0];
        System.out.println("SchoolOrgMapper : " + fileName);
    }

    //MultipleInputs，多个混合数据类型的输入文件集合
    private String getFilePath(Context context) throws IOException {
        // FileSplit fileSplit = (FileSplit) context.getInputSplit();
        InputSplit split = context.getInputSplit();
        Class<? extends InputSplit> splitClass = split.getClass();
        FileSplit fileSplit = null;
        if (splitClass.equals(FileSplit.class)) {
            fileSplit = (FileSplit) split;
        } else if (splitClass.getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
            // begin reflection hackery...
            try {
                Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
                getInputSplitMethod.setAccessible(true);
                fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
            } catch (Exception e) {
                // wrap and re-throw error
                e.printStackTrace();
            }
        // end reflection hackery
        }
        return fileSplit.getPath().getName().toString();
    }

    @Override
    //SimpleGroup,读取parquet文件
    //Text，读取txt文件
    protected void map(Object key, SimpleGroup value, Context context)
            throws IOException, InterruptedException {

        String provinceId = value.getString("province_id",0);
        String cityId = value.getString("city_id",0);
        String districtId = value.getString("district_id",0);
        String schoolId = value.getString("school_id",0);
        String userId = value.getString("user_id",0);

        //"\t""\n"换行符一个字节
        //System.out.println("DwsUcUserOrganizationMapper : key-"+ key + " : value-" + value.toString());
        System.out.println("map处理的inputsplit ："+fileName);
        System.out.println("DwsUcUserOrganizationMapper : "+provinceId+" : "+cityId+" : "+cityId+" : "+districtId+" : "+schoolId+" : "+userId);

        context.write(new MyKeyPair(userId),new DwsUcUserOrganizationBean(provinceId,cityId,districtId,schoolId,userId,"bg-1","DwsUcUserOrganization"));

    }

}

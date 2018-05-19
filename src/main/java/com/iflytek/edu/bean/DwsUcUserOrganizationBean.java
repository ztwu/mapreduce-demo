package com.iflytek.edu.bean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/6
 * Time: 14:01
 * Description
 */

public class DwsUcUserOrganizationBean implements WritableComparable<DwsUcUserOrganizationBean> {

    private String provinceId;
    private String cityId;
    private String districtId;
    private String schoolId;
    private String userId;
    private String appKey;

    private String flag;

    //在反序列化时，反射机制需要调用空参构造函数
    public DwsUcUserOrganizationBean(){
        super();
    }

    //为了初始化方便加入带参数构造函数
    public DwsUcUserOrganizationBean(String provinceId, String cityId, String districtId, String schoolId, String userId, String appKey, String flag) {
        this.provinceId = provinceId;
        this.cityId = cityId;
        this.districtId = districtId;
        this.schoolId = schoolId;
        this.userId = userId;
        this.appKey = appKey;
        this.flag = flag;
    }

    //重写序列化方法
    //UTF==》string类型
    public void write(DataOutput dataOutput) throws IOException {
        //用与平台无关的方式使用UTF-8编码将一个字符串写入输出流
        dataOutput.writeUTF(provinceId);
        dataOutput.writeUTF(cityId);
        dataOutput.writeUTF(districtId);
        dataOutput.writeUTF(schoolId);
        dataOutput.writeUTF(userId);
        dataOutput.writeUTF(appKey);
        dataOutput.writeUTF(flag);
    }

    //重写反序列化方法
    public void readFields(DataInput dataInput) throws IOException {
        provinceId = dataInput.readUTF();
        cityId = dataInput.readUTF();
        districtId = dataInput.readUTF();
        schoolId = dataInput.readUTF();
        userId = dataInput.readUTF();
        appKey = dataInput.readUTF();
        flag = dataInput.readUTF();
    }

    public String getProvinceId() {
        return provinceId;
    }

    public void setProvinceId(String provinceId) {
        this.provinceId = provinceId;
    }

    public String getCityId() {
        return cityId;
    }

    public void setCityId(String cityId) {
        this.cityId = cityId;
    }

    public String getDistrictId() {
        return districtId;
    }

    public void setDistrictId(String districtId) {
        this.districtId = districtId;
    }

    public String getSchoolId() {
        return schoolId;
    }

    public void setSchoolId(String schoolId) {
        this.schoolId = schoolId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAppKey() {
        return appKey;
    }

    public void setAppKey(String appKey) {
        this.appKey = appKey;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    //bean作为key时按照该方法规则排序
    //java 两种比较器
    //Comparable 定义在 类的内部:
    public int compareTo(DwsUcUserOrganizationBean o) {
        return 0;
    }

    @Override
    //重写toString方法
    public String toString() {
        return provinceId+"\t"+cityId+"\t"+districtId+"\t"+schoolId+"\t"+userId+"\t"+appKey+"\t"+flag;
    }

}

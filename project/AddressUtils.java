package com.edu.neusoft.project;

import java.util.HashMap;
import java.util.Map;

public class AddressUtils {

    /**
     * iso 3316-2编码： 参考 https://zh-min-nan.wikipedia.org/wiki/ISO_3166-2:CN
     */
    private static Map<String, String> provinceCodeMap = new HashMap<>();
    static {
        provinceCodeMap.put("北京", "CN-11");
        provinceCodeMap.put("天津", "CN-12");
        provinceCodeMap.put("河北", "CN-13");
        provinceCodeMap.put("山西", "CN-14");
        provinceCodeMap.put("内蒙古", "CN-15");
        provinceCodeMap.put("辽宁", "CN-21");
        provinceCodeMap.put("吉林", "CN-22");
        provinceCodeMap.put("黑龙江", "CN-23");
        provinceCodeMap.put("上海", "CN-31");
        provinceCodeMap.put("江苏", "CN-32");
        provinceCodeMap.put("浙江", "CN-33");
        provinceCodeMap.put("安徽", "CN-34");
        provinceCodeMap.put("福建", "CN-35");
        provinceCodeMap.put("江西", "CN-36");
        provinceCodeMap.put("山东", "CN-37");
        provinceCodeMap.put("河南", "CN-41");
        provinceCodeMap.put("湖北", "CN-42");
        provinceCodeMap.put("湖南", "CN-43");
        provinceCodeMap.put("广东", "CN-44");
        provinceCodeMap.put("广西", "CN-45");
        provinceCodeMap.put("海南", "CN-46");
        provinceCodeMap.put("重庆", "CN-50");
        provinceCodeMap.put("四川", "CN-51");
        provinceCodeMap.put("贵州", "CN-52");
        provinceCodeMap.put("云南", "CN-53");
        provinceCodeMap.put("西藏", "CN-54");
        provinceCodeMap.put("陕西", "CN-61");
        provinceCodeMap.put("甘肃", "CN-62");
        provinceCodeMap.put("青海", "CN-63");
        provinceCodeMap.put("宁夏", "CN-64");
        provinceCodeMap.put("新疆", "CN-65");
        provinceCodeMap.put("台湾", "CN-71");
        provinceCodeMap.put("香港", "CN-91");
        provinceCodeMap.put("澳门", "CN-92");
    }


    public static String getIso3316CodeByProvince(String province) {
        return provinceCodeMap.getOrDefault(province, "");
    }

}

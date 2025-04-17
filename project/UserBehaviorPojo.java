package com.edu.neusoft.project;


/**
 * 源文件格式： user_id,item_id,cat_id,merchant_id,brand_id,month,day,action,age_range,gender,province,
 */
public class UserBehaviorPojo {

    private String  userId;

    private String itemId;

    private String catId;

    private String merchantId;

    private String brandId;

    private String action;

    private String ageRange;

    private String gender;

    private String province;

    private Long timestamp;

    public UserBehaviorPojo() {
    }

    /**
     * 根据csv文件中的一行数据来构造对象
     */
    public UserBehaviorPojo(String line) {
        String[] words = line.split(",");
        this.userId = words[0];
        this.itemId = words[1];
        this.catId = words[2];
        this.merchantId = words[3];
        this.brandId = words[4];
        this.action = words[7];
        this.ageRange = words[8];
        this.gender = words[9];
        this.province = words[10];
        this.timestamp = System.currentTimeMillis();
    }


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getCatId() {
        return catId;
    }

    public void setCatId(String catId) {
        this.catId = catId;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public String getBrandId() {
        return brandId;
    }

    public void setBrandId(String brandId) {
        this.brandId = brandId;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getAgeRange() {
        return ageRange;
    }

    public void setAgeRange(String ageRange) {
        this.ageRange = ageRange;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehaviorPojo{" +
                "userId='" + userId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", catId='" + catId + '\'' +
                ", merchantId='" + merchantId + '\'' +
                ", brandId='" + brandId + '\'' +
                ", action='" + action + '\'' +
                ", ageRange='" + ageRange + '\'' +
                ", gender='" + gender + '\'' +
                ", province='" + province + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}


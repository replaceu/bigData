package foreo.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderInfoDto {
    private Long id;
    private Long provinceId;
    private String orderStatus;
    private Long userId;
    private BigDecimal totalAmount;
    private BigDecimal activityReduceAmount;
    private BigDecimal couponReduceAmount;
    private BigDecimal originalTotalAmount;
    private BigDecimal freightFee;
    private String expireTime;
    private String createTime;
    private String operateTime;
    private String createDate; // 把其他字段处理得到
    private String createHour;
    private Long createTs;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getProvinceId() {
        return provinceId;
    }

    public void setProvinceId(Long provinceId) {
        this.provinceId = provinceId;
    }

    public String getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus == null ? null : orderStatus.trim();
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount;
    }

    public BigDecimal getActivityReduceAmount() {
        return activityReduceAmount;
    }

    public void setActivityReduceAmount(BigDecimal activityReduceAmount) {
        this.activityReduceAmount = activityReduceAmount;
    }

    public BigDecimal getCouponReduceAmount() {
        return couponReduceAmount;
    }

    public void setCouponReduceAmount(BigDecimal couponReduceAmount) {
        this.couponReduceAmount = couponReduceAmount;
    }

    public BigDecimal getOriginalTotalAmount() {
        return originalTotalAmount;
    }

    public void setOriginalTotalAmount(BigDecimal originalTotalAmount) {
        this.originalTotalAmount = originalTotalAmount;
    }

    public BigDecimal getFreightFee() {
        return freightFee;
    }

    public void setFreightFee(BigDecimal freightFee) {
        this.freightFee = freightFee;
    }

    public String getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(String expireTime) {
        this.expireTime = expireTime == null ? null : expireTime.trim();
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime == null ? null : createTime.trim();
    }

    public String getOperateTime() {
        return operateTime;
    }

    public void setOperateTime(String operateTime) {
        this.operateTime = operateTime == null ? null : operateTime.trim();
    }

    public String getCreateDate() {
        return createDate;
    }

    public void setCreateDate(String createDate) {
        this.createDate = createDate == null ? null : createDate.trim();
    }

    public String getCreateHour() {
        return createHour;
    }

    public void setCreateHour(String createHour) {
        this.createHour = createHour == null ? null : createHour.trim();
    }

    public Long getCreateTs() {
        return createTs;
    }

    public void setCreateTs(Long createTs) {
        this.createTs = createTs;
    }
}

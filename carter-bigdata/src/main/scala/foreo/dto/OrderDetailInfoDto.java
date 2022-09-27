package foreo.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderDetailInfoDto {
    private Long id;

    private Long orderId;

    private Long skuId;

    private BigDecimal orderPrice;

    private Long skuNum;

    private String skuName;

    private String createTime;

    private BigDecimal splitTotalAmount;

    private BigDecimal splitActivityAmount;

    private BigDecimal splitCouponAmount;

    private Long createTs;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Long getSkuId() {
        return skuId;
    }

    public void setSkuId(Long skuId) {
        this.skuId = skuId;
    }

    public BigDecimal getOrderPrice() {
        return orderPrice;
    }

    public void setOrderPrice(BigDecimal orderPrice) {
        this.orderPrice = orderPrice;
    }

    public Long getSkuNum() {
        return skuNum;
    }

    public void setSkuNum(Long skuNum) {
        this.skuNum = skuNum;
    }

    public String getSkuName() {
        return skuName;
    }

    public void setSkuName(String skuName) {
        this.skuName = skuName == null ? null : skuName.trim();
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime == null ? null : createTime.trim();
    }

    public BigDecimal getSplitTotalAmount() {
        return splitTotalAmount;
    }

    public void setSplitTotalAmount(BigDecimal splitTotalAmount) {
        this.splitTotalAmount = splitTotalAmount;
    }

    public BigDecimal getSplitActivityAmount() {
        return splitActivityAmount;
    }

    public void setSplitActivityAmount(BigDecimal splitActivityAmount) {
        this.splitActivityAmount = splitActivityAmount;
    }

    public BigDecimal getSplitCouponAmount() {
        return splitCouponAmount;
    }

    public void setSplitCouponAmount(BigDecimal splitCouponAmount) {
        this.splitCouponAmount = splitCouponAmount;
    }

    public Long getCreateTs() {
        return createTs;
    }

    public void setCreateTs(Long createTs) {
        this.createTs = createTs;
    }

}

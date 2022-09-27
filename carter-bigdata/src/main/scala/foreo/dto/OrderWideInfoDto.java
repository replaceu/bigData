package foreo.dto;

import lombok.Data;
import org.apache.commons.lang3.ObjectUtils;


import java.math.BigDecimal;

@Data
public class OrderWideInfoDto {
    Long detailId;
    Long orderId;
    Long skuId;
    BigDecimal orderPrice;
    Long skuNum;
    String skuName;
    Long provinceId;
    String orderStatus;
    Long userId;
    BigDecimal totalAmount;
    BigDecimal activityReduceAmount;
    BigDecimal couponReduceAmount;
    BigDecimal originalTotalAmount;
    BigDecimal freightFee;
    BigDecimal splitFreightFee;
    BigDecimal splitActivityAmount;
    BigDecimal splitCouponAmount;
    BigDecimal splitTotalAmount;
    String expireTime;
    String createTime; //yyyy-MM-dd HH:mm:ss
    String operateTime;
    String createDate; // 把其他字段处理得到
    String createHour;
    String provinceName;//查询维表得到
    String provinceAreaCode;
    String provinceIsoCode;
    String provinceToCode;
    Integer userAge;
    String userGender;
    Long spuId; //作为维度数据 要关联进来
    Long tmId;
    Long category3Id;
    String spuName;
    String tmName;
    String category3Name;

    public OrderWideInfoDto(OrderInfoDto orderInfoDto,OrderDetailInfoDto detailInfoDto) {
        mergeOrderInfo(orderInfoDto);
        mergeDetailInfoDto(detailInfoDto);
    }

    private void mergeDetailInfoDto(OrderDetailInfoDto detailInfoDto) {
        if (detailInfoDto!=null){
            this.detailId = detailInfoDto.getId();
            this.skuId = detailInfoDto.getSkuId();
            this.skuName = detailInfoDto.getSkuName();
            this.orderPrice = detailInfoDto.getOrderPrice();
            this.skuNum = detailInfoDto.getSkuNum();
            this.splitActivityAmount = detailInfoDto.getSplitActivityAmount();
            this.splitCouponAmount = detailInfoDto.getSplitCouponAmount();
            this.splitTotalAmount = detailInfoDto.getSplitTotalAmount();
        }
    }

    private void mergeOrderInfo(OrderInfoDto orderInfo) {
        if (orderInfo!=null){
            this.orderId = orderInfo.getId();
            this.orderStatus = orderInfo.getOrderStatus();
            this.createTime = orderInfo.getCreateTime();
            this.createDate = orderInfo.getCreateDate();
            this.createHour = orderInfo.getCreateHour();
            this.activityReduceAmount = orderInfo.getActivityReduceAmount();
            this.couponReduceAmount = orderInfo.getCouponReduceAmount();
            this.originalTotalAmount = orderInfo.getOriginalTotalAmount();
            this.freightFee = orderInfo.getFreightFee();
            this.totalAmount = orderInfo.getTotalAmount();
            this.provinceId = orderInfo.getProvinceId();
            this.userId = orderInfo.getUserId();
        }
    }


    private void mergeOrderWide(OrderWideInfoDto orderWide){
        this.detailId = ObjectUtils.firstNonNull(this.detailId);
        this.orderId = ObjectUtils.firstNonNull(this.orderId);
        this.skuId = ObjectUtils.firstNonNull(this.skuId);
        this.orderPrice = ObjectUtils.firstNonNull(this.orderPrice);
        this.skuNum =ObjectUtils.firstNonNull(this.skuNum);
        this.skuName = ObjectUtils.firstNonNull(this.skuName);
        this.provinceId = ObjectUtils.firstNonNull(this.provinceId);
        this.orderStatus = ObjectUtils.firstNonNull(this.orderStatus);
        this.userId = ObjectUtils.firstNonNull(this.userId);
        this.totalAmount = ObjectUtils.firstNonNull(this.totalAmount);
        this.activityReduceAmount = ObjectUtils.firstNonNull(this.activityReduceAmount);
        this.couponReduceAmount = ObjectUtils.firstNonNull(this.couponReduceAmount);
        this.originalTotalAmount = ObjectUtils.firstNonNull(this.originalTotalAmount);
        this.freightFee = ObjectUtils.firstNonNull(this.freightFee);
        this.splitFreightFee = ObjectUtils.firstNonNull(this.splitFreightFee);
        this.splitActivityAmount = ObjectUtils.firstNonNull(this.splitActivityAmount);
        this.splitCouponAmount = ObjectUtils.firstNonNull(this.splitCouponAmount);
        this.splitTotalAmount = ObjectUtils.firstNonNull(this.splitTotalAmount);
        this.expireTime = ObjectUtils.firstNonNull(this.expireTime);
        this.createTime = ObjectUtils.firstNonNull(this.createTime);
        this.operateTime = ObjectUtils.firstNonNull(this.operateTime);
        this.createDate = ObjectUtils.firstNonNull(this.createDate);
        this.createHour = ObjectUtils.firstNonNull(this.createHour);
        this.provinceName = ObjectUtils.firstNonNull(this.provinceName);
        this.provinceAreaCode = ObjectUtils.firstNonNull(this.provinceAreaCode);
        this.provinceIsoCode = ObjectUtils.firstNonNull(this.provinceIsoCode);
        this.provinceToCode = ObjectUtils.firstNonNull(this.provinceToCode);
        this.userAge = ObjectUtils.firstNonNull(this.userAge);
        this.userGender = ObjectUtils.firstNonNull(this.userGender);
        this.spuId = ObjectUtils.firstNonNull(this.spuId);
        this.tmId = ObjectUtils.firstNonNull(this.tmId);
        this.category3Id = ObjectUtils.firstNonNull(this.category3Id);
        this.spuName = ObjectUtils.firstNonNull(this.spuName);
        this.tmName = ObjectUtils.firstNonNull(this.tmName);
        this.category3Name = ObjectUtils.firstNonNull(this.category3Name);
    }
}

package foreo.bean

case class OrderWideDo(
                        var provinceId: Long=0L, //省份id
                        var orderStatus: String=null,  //订单状态
                        var userId: Long=0L, //用户id
                        var finalTotalAmount: Double=0D,  //总金额
                        var benefitReduceAmount: Double=0D, //优惠金额
                        var originalTotalAmount: Double=0D, //原价金额
                        var freightFee: Double=0D,  //运费
                        var expireTime: String=null, //失效时间
                        var createTime: String=null, //创建时间
                        var operateTime: String=null,  //操作时间
                        var createDate: String=null, //创建日期
                        var createHour: String=null, //创建小时
                        var ifFirstOrder:String=null, //是否首单
                        var provinceName:String=null,  //地区名
                        var provinceAreaCode:String=null, //地区编码
                        var provinceIsoCode:String=null, //国际地区编码
                        var userAgeGroup:String=null, //用户年龄段
                        var userGender:String=null,//用户性别
                        var orderDetailId: Long=0L,
                        var orderId:Long=0L,
                        var skuId: Long=0L,
                        var orderPrice: Double=0D,
                        var skuNum:Long=0L,
                        var skuName: String=null,
                        var spuId: Long=0L,//作为维度数据 要关联进来
                        var tmId: Long=0L,
                        var category3Id: Long=0L,
                        var spuName: String=null,
                        var tmName: String=null,
                        var category3Name: String=null,
                        //分摊金额
                        var finalDetailAmount: Double = 0D,

                      )
{

  def mergeOrderDo(orderDo: OrderDo) = {
    if(orderDo!=null){
      this.orderId = orderDo.id
      this.orderStatus = orderDo.orderStatus
      this.createTime = orderDo.createTime
      this.benefitReduceAmount=orderDo.benefitReduceAmount
      this.originalTotalAmount =orderDo.originalTotalAmount
      this.freightFee = orderDo.freightFee
      this.finalTotalAmount = orderDo.finalTotalAmount
      this.provinceName = orderDo.provinceName
      this.provinceAreaCode = orderDo.provinceAreaCode
      this.userAgeGroup  = orderDo.userAgeGroup
      this.userGender = orderDo.userGender
      this.ifFirstOrder = orderDo.ifFirstOrder
      this.userId = orderDo.userId
    }
    
  }

  def mergeDetailDo(orderDetailDo: OrderDetailDo): Unit = {
    if(orderDetailDo!=null){
      this.orderDetailId = orderDetailDo.id
      this.skuId = orderDetailDo.skuId
      this.skuName = orderDetailDo.skuName
      this.orderPrice = orderDetailDo.orderPrice
      this.skuNum = orderDetailDo.skuNum
      this.spuId = orderDetailDo.spuId
      this.tmId =orderDetailDo.tmId
      this.category3Id = orderDetailDo.category3Id
      this.category3Name = orderDetailDo.category3Name
      this.spuName = orderDetailDo.spuName
      this.tmName = this.tmName
    }
    
  }
  

  def this(orderDo: OrderDo, orderDetailDo: OrderDetailDo){
    this
    mergeOrderDo(orderDo)
    mergeDetailDo(orderDetailDo)
  }

}

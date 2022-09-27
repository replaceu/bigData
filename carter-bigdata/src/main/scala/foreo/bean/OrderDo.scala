package foreo.bean

case class OrderDo(
                    id: Long,  //订单编号
                    provinceId: Long, //省份id
                    orderStatus: String,  //订单状态
                    userId: Long, //用户id
                    finalTotalAmount: Double,  //总金额
                    benefitReduceAmount: Double, //优惠金额
                    originalTotalAmount: Double, //原价金额
                    freightFee: Double,  //运费
                    expireTime: String, //失效时间
                    createTime: String, //创建时间
                    operateTime: String,  //操作时间
                    var createDate: String, //创建日期
                    var createHour: String, //创建小时
                    var ifFirstOrder:String, //是否首单
                    var provinceName:String,  //地区名
                    var provinceAreaCode:String, //地区编码
                    var provinceIsoCode:String, //国际地区编码
                    var userAgeGroup:String, //用户年龄段
                    var userGender:String //用户性别
                  )

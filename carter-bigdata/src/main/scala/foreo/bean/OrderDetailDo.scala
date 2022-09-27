package foreo.bean

case class OrderDetailDo(
                          id: Long,
                          orderId:Long,
                          skuId: Long,
                          orderPrice: Double,
                          skuNum:Long,
                          skuName: String,
                          createTime: String,
                          var spuId: Long,     //作为维度数据 要关联进来
                          var tmId: Long,
                          var category3Id: Long,
                          var spuName: String,
                          var tmName: String,
                          var category3Name: String
                        )

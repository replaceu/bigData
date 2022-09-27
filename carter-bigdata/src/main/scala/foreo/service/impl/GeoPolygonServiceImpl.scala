package foreo.service.impl

import foreo.bean.DistrictDo
import foreo.service.{DistrictsService, GeoPolygonService}
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.locationtech.jts.geom.Polygon
import org.locationtech.jts.io.WKTReader
import org.springframework.beans.factory.annotation.Autowired

import java.util
import scala.collection.mutable

class GeoPolygonServiceImpl @Autowired()()extends GeoPolygonService{
  override def changeDistrictToPolygon(districtsBroadcast: Broadcast[java.util.ArrayList[DistrictDo]], reader: WKTReader): mutable.Buffer[(DistrictDo, Polygon)] = {
    //todo:1.获取广播变量
    val districtList: util.ArrayList[DistrictDo] = districtsBroadcast.value
    //todo:2.将Java集合转化成为scala集合
    import scala.collection.JavaConversions._
    val districtPolygonTuple: mutable.Buffer[(DistrictDo, Polygon)] = districtList.map(district => {
      val districtPolygon: String = district.getPolygon
      var wktPolygon = ""
      if (!StringUtils.isEmpty(districtPolygon)) {
        wktPolygon = "POLYGON((" + districtPolygon.replaceAll(",", " ").replaceAll(";", ",") + "))"
        val polygon: Polygon = reader.read(wktPolygon).asInstanceOf[Polygon]
        (district, polygon)
      } else {
        null
      }
    })
    districtPolygonTuple
  }
}

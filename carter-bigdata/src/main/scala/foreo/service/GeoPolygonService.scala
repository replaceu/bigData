package foreo.service

import foreo.bean.DistrictDo
import org.apache.spark.broadcast.Broadcast
import org.locationtech.jts.geom.Polygon
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable

trait GeoPolygonService {
  def changeDistrictToPolygon(districtsBroadcast: Broadcast[java.util.ArrayList[DistrictDo]], reader:WKTReader):mutable.Buffer[(DistrictDo,Polygon)]
}

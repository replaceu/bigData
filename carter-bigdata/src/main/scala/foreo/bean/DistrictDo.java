package foreo.bean;

import java.io.Serializable;

/**
 * 行政区域
 */
public class DistrictDo implements Serializable {
    private String cityCode;

    private String name;

    //行政区域边界的一圈经纬度字符串
    private String polygon;

    public DistrictDo(String cityCode, String name, String polygon) {
        this.cityCode = cityCode;
        this.name = name;
        this.polygon = polygon;
    }

    public String getCityCode() {
        return cityCode;
    }

    public void setCityCode(String cityCode) {
        this.cityCode = cityCode == null ? null : cityCode.trim();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name == null ? null : name.trim();
    }

    public String getPolygon() {
        return polygon;
    }

    public void setPolygon(String polygon) {
        this.polygon = polygon == null ? null : polygon.trim();
    }
}

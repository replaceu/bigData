package foreo.service;

import com.alibaba.fastjson.JSONArray;
import foreo.bean.DistrictDo;


import java.util.List;

public interface DistrictsService {
    JSONArray getCityDistricts(String districtName, String districtCode);

    void parseDistrictInfo(JSONArray districts, String targetCityCode, List<DistrictDo> districtList);

}

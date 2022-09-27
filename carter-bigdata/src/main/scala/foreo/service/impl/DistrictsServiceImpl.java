package foreo.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import foreo.bean.DistrictDo;
import foreo.service.DistrictsService;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Service
public class DistrictsServiceImpl implements DistrictsService {
    //定义高德地图开发者key(前提需要在高德地图开发者中心注册成为开发者，然后申请开发者key)
    private static String key = "ba1539fb52a33d97d645b9692d5fa4b8";

    //传入一个区名称，然后返回这个区的经纬度的边界
    @Override
    public JSONArray getCityDistricts(String districtName, String districtCode) {
        JSONObject retJson = null;
        StringBuffer param = new StringBuffer();
        param.append("keys=").append(key).append("&location=").append("&keywords=" + districtName).append("&subdistrict=1").append("&extensions=all");
        //todo:调用腾旭地图API接口
        String result = "";
        BufferedReader inputReader = null;
        try{
            String requestUrl = "http://restapi.amap.com/v3/geocode/regeo"+"?"+param.toString();
            URLConnection connection = new URL(requestUrl).openConnection();
            //设置通用的请求属性
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            connection.connect();
            //取所有响应头字段
            Map<String, List<String>> map = connection.getHeaderFields();
            // 定义 BufferedReader输入流来读取URL的响应
            inputReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            while ((line = inputReader.readLine()) != null) {
                result += line;
            }
            retJson = JSONObject.parseObject(result);

        }catch (Exception e){
            e.printStackTrace();
        }

        return retJson.getJSONArray("districts");
    }

    @Override
    public void parseDistrictInfo(JSONArray districts, String targeCityCode, List<DistrictDo> districtList) {
        if (null != districts && (!districts.isEmpty())) {

            Iterator<Object> iterator = districts.iterator();
            while (iterator.hasNext()) {
                JSONObject next = (JSONObject) (iterator.next());

                String name = next.getString("name");
                String cityCode = next.getString("cityCode");
                String polyline = next.getString("polyline");
                String level = next.getString("level");
                if (level.equals("city")) {
                    //todo:如果级别是城市,继续进行区/县解析
                    JSONArray district = next.getJSONArray("districts");
                    parseDistrictInfo(district, null,districtList);
                } else if (level.equals("district")) {
                    if (null != targeCityCode && targeCityCode.equals(cityCode)) {
                        districtList.add(new DistrictDo(cityCode,name,polyline));
                        return;
                    }

                    if (null == polyline) {
                        JSONArray cityDistrict = getCityDistricts(name, null);
                        parseDistrictInfo(cityDistrict, cityCode,districtList);
                    }
                    continue;
                }
            }
        }
    }
}

package foreo.controller

import foreo.service.DriverTravelService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{PostMapping, RequestBody, RequestMapping}

@Controller
@RequestMapping(Array("/foreoDriverTravel"))
class DriverTravelController @Autowired()(driverTravelService: DriverTravelService) {
  @RequestBody
  @PostMapping(Array("/driverOrderInfo"))
  def getDriverOrderToHbase()={
    driverTravelService.getDriverOrderToHbase()
  }

}

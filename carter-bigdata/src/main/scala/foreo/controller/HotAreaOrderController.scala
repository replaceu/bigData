package foreo.controller

import foreo.service.{TravelOrderService, UserDayService}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{GetMapping, PostMapping, RequestBody, RequestMapping}

@Controller
@RequestMapping(Array("/foreoOrder"))
class HotAreaOrderController@Autowired()(travelOrderService: TravelOrderService) {
  @RequestBody
  @PostMapping(Array("/travelHotAreaOrderInfo"))
  def getHorAreaOrderInfo(): Unit ={
    travelOrderService.getHotAreaOrderToHbase()
  }
}

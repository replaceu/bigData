package foreo.controller


import foreo.service.UserDayService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{GetMapping, PostMapping, RequestBody, RequestMapping}

@Controller
@RequestMapping(Array("/foreoUserInfo"))
class UserDayController @Autowired()(userDayService:UserDayService){
  @RequestBody
  @GetMapping(Array("/userFirstLogin"))
  def getUserDailyLoginInfo(): Unit ={
    userDayService.getUserDailyLoginInfo()
  }

}

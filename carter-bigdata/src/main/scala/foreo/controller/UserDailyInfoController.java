package foreo.controller;

import foreo.service.UserInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;


@Controller
@RequestMapping("/userDailyInfo")
public class UserDailyInfoController {
    @Autowired
    UserInfoService userInfoService;

    @PostMapping(value = "getUserDailyFirstVisit")
    public void getUserDailyFirstVisit() {
         userInfoService.getUserDailyFirstVisit();
    }

}

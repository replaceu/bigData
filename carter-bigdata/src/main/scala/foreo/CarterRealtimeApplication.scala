package foreo

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class AppConfig

object CarterRealtimeApplication extends App {
  SpringApplication.run(classOf[AppConfig])
}

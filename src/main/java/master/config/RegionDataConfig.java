package master.config;

import master.pojo.Region;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class RegionDataConfig {
    List<Region> regionList;
//初始化region列表
    @Bean
    public List<Region> init(){
        regionList = new ArrayList<>();
        return regionList;
    }

}

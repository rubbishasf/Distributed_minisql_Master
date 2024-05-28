package master.synchronize;

import master.pojo.Region;
import master.service.ZKService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
@Component
public class DataSynchronize implements Runnable{
    @Autowired
    List <Region> regions;
    @Autowired
    ZKService zkService;
    public void run(){
        //刷新region缓存
        regions.clear();
        regions.addAll(zkService.getAllRegions());
    }


}

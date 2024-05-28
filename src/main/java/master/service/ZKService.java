package master.service;

import com.alibaba.fastjson2.JSONObject;
import jakarta.annotation.PostConstruct;
import master.pojo.Master;
import master.pojo.Region;
import master.utils.NetUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Service
public class ZKService {

    @Autowired
    List<Region> regions;
    @Autowired
    CuratorFramework curatorFramework;
    @Autowired
    NetUtils netUtils;
    @Value("${server.port}")
    String serverPort;
    @PostConstruct
    public void init(){
        try {
            listener();
            List<String> children = curatorFramework.getChildren().forPath("/region_parent");
            for (String child : children) {
                //将region信息放入内存
                byte[] data = curatorFramework.getData().forPath("/region_parent/" + child);
                Region region = new Region();
                region.deserializeFromString(new String (data));
                regions.add(region);

            }
            //创建master节点
            Master master = new Master();
            master.setHost(java.net.InetAddress.getLocalHost().getHostAddress());
            master.setPort(serverPort);
            if (curatorFramework.checkExists().forPath("/master") == null) {
                System.out.println("create master");
                curatorFramework.create().creatingParentsIfNeeded().forPath("/master");
            }
            curatorFramework.setData().forPath("/master", (master.getHost() + "," + master.getPort()).getBytes());

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public List<Region> getAllRegions(){
        try {

            List<String> children = curatorFramework.getChildren().forPath("/region_parent");
            List<Region> regionList = new ArrayList<>();
            for (String child : children) {
                byte[] data = curatorFramework.getData().forPath("/region_parent/" + child);
                Region region = new Region();
                region.deserializeFromString(new String (data));
                regionList.add(region);
            }
            return regionList;
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    public void refreshRegion() {
        try {
            regions.clear();
            regions.addAll(getAllRegions());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void listener(){
        try {
            //线程调用删除从节点中的表
            ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(3);

            CuratorCache curatorCache = CuratorCache.build(curatorFramework, "/region_parent");
            curatorCache.listenable().addListener(CuratorCacheListener.builder()
                    .forInitialized(() -> {
                        System.out.println("listener initialized");
                    })
                    .forCreates(newRegion -> {

                        //创建新节点
                        if (newRegion == null || newRegion.getData() == null)
                            return;
                        refreshRegion();
                        System.out.println("[listener]: create node " + new String(newRegion.getData()));

                    })
                    .forDeletes(deletedRegion -> {
                        //删除节点
                        System.out.println("[listener]: delete" + new String(deletedRegion.getData()));
                        refreshRegion();
                        Region region = null;
                        try {
                            region = new Region();
                        } catch (UnknownHostException e) {
                            throw new RuntimeException(e);
                        }
                        region.deserializeFromString(new String(deletedRegion.getData()));
                        List<String> tables = region.getTables();

                        for (String table : tables) {

                            Region regionMain = null;
                            if (table.endsWith("_slave")) {

                                try {
                                    regionMain = new Region();
                                } catch (UnknownHostException e) {
                                    throw new RuntimeException(e);
                                }
                                String tableMain = table.substring(0, table.length() - 6);
                                for (Region regiontmp : regions) {
                                    if (regiontmp.getTables().contains(tableMain)) {
                                        regionMain.deserializeFromString(regiontmp.toZKNodeValue());
                                        break;
                                    }
                                }

                                //节点中存的是从表
                                refreshRegion();
                                Collections.shuffle(regions);
                                for (Region region1 : regions) {
                                    if (!region1.containsTable(table) && !region1.containsTable(tableMain)) {
                                        //此region既没有主表也没有从表,发送请求使其存放从表
                                        String url = "http://" + region1.getIp() + ":" + region1.getPort() + "/backups/" + regionMain.toZKNodeValue() + "/" + tableMain;
                                        netUtils.sendPost(url);
                                        break;

                                    }
                                }

                            } else {//主表
                                //将一个从节点变为主节点

                                for (Region region1 : regions) {
                                    if (region1.containsTable(table + "_slave")) {
                                        region1.getTables().remove(table + "_slave");
                                        region1.getTables().add(table);

                                        try {
                                            curatorFramework.setData().forPath("/region_parent/" + region1.getRegionName(), region1.toZKNodeValue().getBytes());
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                        try {
                                            regionMain = new Region();
                                        } catch (UnknownHostException e) {
                                            throw new RuntimeException(e);
                                        }

                                        regionMain.deserializeFromString(region1.toZKNodeValue());
                                        break;
                                    }
                                }

                                refreshRegion();
                                Collections.shuffle(regions);
                                for (Region region1 : regions) {
                                    if (!region1.containsTable(table) && !region1.containsTable(table + "_slave")) {
                                        //此region既没有主表也没有从表,发送请求使其存放从表
                                        String url = "http://" + region1.getIp() + ":" + region1.getPort() + "/backups/" + regionMain.toZKNodeValue() + "/" + table;
                                        netUtils.sendPost(url);
                                        break;

                                    }


                                }
                            }

                        }
                    })
                    .forChanges((oldData, newData) -> {
                        try {
                            refreshRegion();
                            System.out.println("[listener]: change" + new String(oldData.getData()) + " to " + new String(newData.getData()));
                            Region oldRegion = null;
                            Region newRegion = null;

                            oldRegion = new Region();
                            newRegion = new Region();


                            oldRegion.deserializeFromString(new String(oldData.getData()));
                            newRegion.deserializeFromString(new String(newData.getData()));
                            if (oldRegion.getTables().size() == newRegion.getTables().size())//数据更新不需要做任何事
                                return;
                                //删除表
                            else if (oldRegion.getTables().size() > newRegion.getTables().size()) {
                                String tableName = null;
                                for (String table : oldRegion.getTables()) {
                                    if (!newRegion.containsTable(table))
                                        tableName = table;
                                }
                                if (tableName.endsWith("_slave"))
                                    return;
                                refreshRegion();
                                for (Region region : regions) {
                                    if (region.containsTable(tableName + "_slave")) {
                                        region.getTables().remove(tableName + "_slave");

                                        try {
                                            curatorFramework.setData().forPath("/region_parent/" + region.getRegionName(), region.toZKNodeValue().getBytes());
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }

                                        String sql = "drop table " + tableName;
                                        String newsql = sql.replace(' ', '@');
                                        String url = "http://" + region.getIp() + ":" + region.getPort() + "/exec/" + newsql;
                                        netUtils.sendPost(url);


                                    }

                                }

                            } else {//新建表
                                String tableName = newRegion.getTables().get(newRegion.getTables().size() - 1);
                                if (tableName.endsWith("_slave"))
                                    return;
                                //添加两个从表
                                refreshRegion();
                                Collections.shuffle(regions);
                                int count = 0;
                                Region regionMain = new Region();
                                for (Region region : regions) {

                                    if (region.getTables().contains(tableName)) {
                                        regionMain.deserializeFromString(region.toZKNodeValue());
                                    }
                                }
                                for (Region region1 : regions) {
                                    if (!region1.containsTable(tableName) && !region1.containsTable(tableName + "_slave")) {

                                        System.out.println("slave table to create in : " + region1.toZKNodeValue());
                                        //此region既没有主表也没有从表,发送请求使其存放从表
                                        count++;

                                        String url = "http://" + region1.getIp() + ":" + region1.getPort() + "/backups/" + regionMain.toZKNodeValue() + "/" + tableName;
                                        System.out.println("send post to " + url);
                                        netUtils.sendPost(url);
                                        if (count == 2)
                                            break;
                                    }

                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }


                    })
                    .build());
            curatorCache.start();
        } catch (Exception e){
            e.printStackTrace();
        }

    }




}

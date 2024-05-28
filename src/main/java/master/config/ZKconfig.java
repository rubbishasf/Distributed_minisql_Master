package master.config;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class ZKconfig {

    private CuratorFramework curatorFramework;
    @Value("${zookeeper.url}")
    private String url;
    @Value("${zookeeper.namespace}")
    private String namespace;


    @Bean
    public CuratorFramework RegisterMaster() throws Exception {

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 5);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(url)
                .namespace(namespace)
                .retryPolicy(retryPolicy)
                .build();
        curatorFramework.start();
        this.curatorFramework = curatorFramework;
        //所有region节点的父节点
        if (curatorFramework.checkExists().forPath("/region_parent") == null) {
            System.out.println("create region_parent!");
            curatorFramework.create().creatingParentsIfNeeded().forPath("/region_parent");
        }
        return curatorFramework;

    }


}

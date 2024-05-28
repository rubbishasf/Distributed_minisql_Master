package master.utils;

import com.alibaba.fastjson2.JSONObject;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;

@Component
public class NetUtils {
    public JSONObject sendPost(String url) {
        RestTemplate client = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<JSONObject> httpEntity = new HttpEntity<>(null, headers);
        return client.postForEntity(url, httpEntity, JSONObject.class).getBody();
    }
}

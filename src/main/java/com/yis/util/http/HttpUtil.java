package com.yis.util.http;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * March, or die.
 *
 * @Description: http工具类
 * @Created by yisany on 2020/01/07
 */
public class HttpUtil {

    private static final Logger logger = LogManager.getLogger(HttpUtil.class);

    private HttpUtil() {}

    /**
     * 发送get请求
     * @param url 请求url
     * @return
     */
    public static String get(String url) {
        try {
            HttpGet request = new HttpGet(url);
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpResponse response = httpClient.execute(request);
            if (logger.isDebugEnabled()) {
                logger.debug("HttpRequestUtil sendGet， response code={}", response.getStatusLine().getStatusCode());
            }

            if (response == null) {
                return null;
            } else {
                if (response.getStatusLine().getStatusCode() > 300) {
                    logger.warn("HttpRequestUtil sendGet, response code={}", response.getStatusLine().getStatusCode());
                }

                return EntityUtils.toString(response.getEntity(), "utf-8");
            }
        } catch (Exception var4) {
            logger.warn("HttpRequestUtil sendGet exception, url={}", url, var4);
            return null;
        }
    }

    /**
     * 发送post请求
     *
     * @param url 网址
     * @param paramMap  post表单数据
     * @return 返回数据
     */
    public static String post(String url, Map<String, Object> paramMap) {
        List<BasicNameValuePair> params = new ArrayList();
        Iterator var3 = paramMap.entrySet().iterator();

        while(var3.hasNext()) {
            Map.Entry<String, String> entry = (Map.Entry)var3.next();
            params.add(new BasicNameValuePair((String)entry.getKey(), (String)paramMap.get(entry.getKey())));
        }

        try {
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost request = new HttpPost(url);
            request.addHeader("content-type", "application/json;charset=utf-8");
            request.setEntity(new UrlEncodedFormEntity(params, "utf-8"));
            HttpResponse response = httpClient.execute(request);
            if (logger.isDebugEnabled()) {
                logger.debug("HttpRequestUtil sendPost， response code={}", response.getStatusLine().getStatusCode());
            }

            if (response == null) {
                return null;
            } else {
                if (response.getStatusLine().getStatusCode() > 300) {
                    logger.warn("HttpRequestUtil sendPost, response code={}", response.getStatusLine().getStatusCode());
                }

                return EntityUtils.toString(response.getEntity(), "utf-8");
            }
        } catch (IOException var6) {
            logger.error("HttpRequestUtil sendPost,url={},param={}", new Object[]{url, params, var6});
            return null;
        }
    }

}

package com.yis.util.encode;

import org.apache.commons.codec.binary.Base64;
import org.bouncycastle.util.encoders.UrlBase64;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * March, or die.
 *
 * @Description: 常见的编码方式
 * @Created by yisany on 2020/03/06
 */
public class CodecUtil {
    public CodecUtil() {
    }

    public static String base64Decode(String input, String encoding) throws UnsupportedEncodingException {
        byte[] output = Base64.decodeBase64(input);
        return new String(output, encoding);
    }

    public static String urlBase64Encode(String input, String encoding) throws UnsupportedEncodingException {
        byte[] b = UrlBase64.encode(input.getBytes(encoding));
        return new String(b, encoding);
    }

    public static String base64Encode(String input, String encoding) throws UnsupportedEncodingException {
        byte[] output = Base64.encodeBase64(input.getBytes(encoding));
        return new String(output);
    }

    public static String urldecode(String input, String encoding) throws UnsupportedEncodingException {
        return URLDecoder.decode(input, encoding);
    }

    public static String urlEncode(String input, String encoding) throws UnsupportedEncodingException {
        return URLEncoder.encode(input, encoding);
    }

}

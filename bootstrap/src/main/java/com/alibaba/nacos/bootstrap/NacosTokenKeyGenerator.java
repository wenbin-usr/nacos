package com.alibaba.nacos.bootstrap;

import java.util.Base64;

public class NacosTokenKeyGenerator {
    public static void main(String[] args) {
        String key = "wenbin-secret";
        byte[] decoded = Base64.getDecoder().decode(key);
        if (decoded.length < 32) {
            System.err.println("非法：解码后仅 " + decoded.length + " 字节，至少需要 32 字节");
        } else {
            System.out.println("合法：解码后 " + decoded.length + " 字节");
        }
    }

}

package com.chungjunming.online.streaming.util;

import com.alibaba.fastjson.JSONObject;

/**
 * Created by Chungjunming on 2019/11/5.
 */
public class ParseJsonData {
    public static JSONObject getJsonData(String data) {
        try {
            return JSONObject.parseObject(data);
        } catch (Exception e) {
            return null;
        }
    }
}

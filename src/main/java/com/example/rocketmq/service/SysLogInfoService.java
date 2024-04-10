package com.example.rocketmq.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * @Author niuzheju
 * @Date 14:56 2024/4/9
 */
@Service
public class SysLogInfoService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SysLogInfoService.class);

    public String selectLogInfo(){
        LOGGER.info("执行本地业务逻辑, 查询日志信息");
        return "";
    }
}

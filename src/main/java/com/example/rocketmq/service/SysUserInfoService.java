package com.example.rocketmq.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * @Author niuzheju
 * @Date 14:55 2024/4/9
 */
@Service
public class SysUserInfoService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SysUserInfoService.class);

    public void saveUser() {
        LOGGER.info("执行本地业务逻辑, 保存用户");
        int i = 10 / 0;
    }
}

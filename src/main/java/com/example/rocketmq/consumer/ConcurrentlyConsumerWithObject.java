package com.example.rocketmq.consumer;

import com.alibaba.fastjson.JSON;
import com.example.rocketmq.bean.OrderBean;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @Author niuzheju
 * @Date 15:45 2024/4/1
 */
@Component
@RocketMQMessageListener(topic = "${topic.orderWithObject}", consumerGroup = "${topic.orderWithObject}_GROUP")
public class ConcurrentlyConsumerWithObject implements RocketMQListener<OrderBean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentlyConsumerWithObject.class);


    @Override
    public void onMessage(OrderBean message) {
        LOGGER.info("message: {}", JSON.toJSONString(message));
    }
}

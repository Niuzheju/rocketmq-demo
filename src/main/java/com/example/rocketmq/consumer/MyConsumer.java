package com.example.rocketmq.consumer;

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
@RocketMQMessageListener(topic = "${topic.order}", consumerGroup = "${rocketmq.consumer.group}")
public class MyConsumer implements RocketMQListener<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyConsumer.class);

    @Override
    public void onMessage(String s) {
        LOGGER.info("接受到消息: {}", s);

    }
}

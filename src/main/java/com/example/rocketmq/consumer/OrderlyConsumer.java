package com.example.rocketmq.consumer;

import org.apache.rocketmq.spring.annotation.ConsumeMode;
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
// selectorExpression通过tag筛选消息, 只消费指定tag的消息, 多个tag使用|分割
@RocketMQMessageListener(topic = "${topic.orderly}", consumerGroup = "ORDERLY_GROUP", selectorExpression = "tag", consumeThreadNumber = 4, consumeMode = ConsumeMode.ORDERLY)
public class OrderlyConsumer implements RocketMQListener<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderlyConsumer.class);

    /**
     * 顺序消费
     *
     * @param s 消息体
     */
    @Override
    public void onMessage(String s) {
        LOGGER.info("接受到消息: {}", s);

    }
}

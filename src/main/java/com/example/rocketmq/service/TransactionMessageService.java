package com.example.rocketmq.service;

import lombok.Setter;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

/**
 * @Author niuzheju
 * @Date 14:44 2024/4/9
 */
@Service
public class TransactionMessageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionMessageService.class);

    @Setter(onMethod_ = @Autowired)
    private RocketMQTemplate rocketMQTemplate;

    @Value("${topic.order}")
    private String orderTopic;

    public void sendTransactionMessage(String id, String message) {
        LOGGER.info("发送事务消息");
        Message<String> msg = MessageBuilder.withPayload(message).setHeader(RocketMQHeaders.KEYS, id).build();
        TransactionSendResult result = rocketMQTemplate.sendMessageInTransaction(orderTopic + ":transaction", msg, id);
        LOGGER.info("result: {}", result);

    }
}

package com.example.rocketmq.controller;

import lombok.Setter;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author niuzheju
 * @Date 15:26 2024/4/1
 */
@RestController
public class ProducerController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerController.class);

    @Setter(onMethod_ = @Autowired)
    private RocketMQTemplate rocketMQTemplate;

    @Value("${topic.order}")
    private String orderTopic;

    @PostMapping("/sendNormalMessage")
    public void sendNormalMessage() {
        Message<String> message = MessageBuilder.withPayload("message, from springboot").build();
        rocketMQTemplate.send(orderTopic, message);
        LOGGER.info("消息发送成功...");
    }
}

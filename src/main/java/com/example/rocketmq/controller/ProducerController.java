package com.example.rocketmq.controller;

import com.example.rocketmq.bean.OrderBean;
import com.example.rocketmq.service.TransactionMessageService;
import lombok.Setter;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * @Author niuzheju
 * @Date 15:26 2024/4/1
 */
@RestController
public class ProducerController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerController.class);

    @Setter(onMethod_ = @Autowired)
    private TransactionMessageService transactionMessageService;

    @Setter(onMethod_ = @Autowired)
    private RocketMQTemplate rocketMQTemplate;

    @Value("${topic.order}")
    private String orderTopic;

    @Value("${topic.orderWithObject}")
    private String orderWithObject;

    @Value("${topic.orderly}")
    private String orderlyTopic;

    @PostMapping("/sendNormalMessage")
    public void sendNormalMessage() {
        for (int i = 0; i < 50; i++) {
            Message<String> message = MessageBuilder.withPayload("message, from springboot" + i).build();
            rocketMQTemplate.send(orderTopic, message);
            LOGGER.info("消息发送成功...");
        }
    }

    @PostMapping("/sendNormalMessageWithObject")
    public void sendNormalMessageWithObject() {
        for (int i = 0; i < 50; i++) {
            OrderBean order = new OrderBean();
            order.setId((long) (i + 1));
            order.setOrderNo("orderNo" + i);
            order.setCount(1L);
            order.setPrice(BigDecimal.valueOf(20 + i));
            order.setOrderTime(LocalDateTime.now());
            order.setDesc("desc");
            rocketMQTemplate.convertAndSend(orderWithObject, order);
            LOGGER.info("消息发送成功...");
        }
    }

    @PostMapping("/sendOrderMessage")
    public void sendOrderMessage() {
        for (int i = 0; i < 50; i++) {
            Message<String> message = MessageBuilder
                    .withPayload("log, orderNo" + i)
                    .build();
            SendResult sendResult = rocketMQTemplate.syncSendOrderly(orderlyTopic + ":tag", message, "log");
            LOGGER.info("i: {}, queueId: {}", i, sendResult.getMessageQueue().getQueueId());
        }

        for (int i = 0; i < 50; i++) {
            Message<String> message = MessageBuilder
                    .withPayload("logistics, orderNo" + i)
                    .build();
            SendResult sendResult = rocketMQTemplate.syncSendOrderly(orderlyTopic, message, "logistics");
            LOGGER.info("i: {}, queueId: {}", i, sendResult.getMessageQueue().getQueueId());
        }
    }

    @PostMapping("/sendAsyncMessage")
    public void sendAsyncMessage() {
        for (int i = 0; i < 50; i++) {
            Message<String> message = MessageBuilder.withPayload("async message" + i).build();
            int finalI = i;
            rocketMQTemplate.asyncSend(orderTopic, message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    LOGGER.info("异步发送消息成功, i: {}, queueId: {}", finalI, sendResult.getMessageQueue().getQueueId());
                }

                @Override
                public void onException(Throwable e) {
                    LOGGER.error("异步发送消息失败", e);

                }
            });
        }

        LOGGER.info("异步发送消息,此方法立即返回");
    }

    @PostMapping("/sendDelayMessage")
    public void sendDelayMessage() {
        for (int i = 0; i < 50; i++) {
            Message<String> message = MessageBuilder.withPayload("delay message" + i).build();
            SendResult sendResult = rocketMQTemplate.syncSendDelayTimeSeconds(orderTopic, message, 30);
            LOGGER.info("i: {}, queueId: {}", i, sendResult.getMessageQueue().getQueueId());
        }
    }

    @PostMapping("/sendTransactionMessage")
    public void sendTransactionMessage() {
        transactionMessageService.sendTransactionMessage("1", "transaction message 001");
    }
}

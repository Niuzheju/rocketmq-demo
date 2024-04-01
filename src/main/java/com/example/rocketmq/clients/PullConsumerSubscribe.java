package com.example.rocketmq.clients;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * PULL模式消费
 * SUBSCRIBE模式
 * 参考: <a href="https://rocketmq.apache.org/zh/docs/4.x/consumer/03pull">PULL消费</a>
 * @Author niuzheju
 * @Date 14:11 2024/3/20
 */
public class PullConsumerSubscribe {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullConsumerSubscribe.class);

    private static volatile boolean running = true;

    public static void main(String[] args) {
        String nameServer = "192.168.56.101:9876";
        String topic = "PULL";
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("PULL_GROUP");
        consumer.setNamesrvAddr(nameServer);
        try {
            consumer.subscribe(topic, "*");
            // 设置每次拉取消息数量, 默认为10
            consumer.setPullBatchSize(20);
            consumer.start();
            while (running) {
                List<MessageExt> messageExtList = consumer.poll();
                LOGGER.info("messageExtList: {}", messageExtList);
            }
        } catch (MQClientException e) {
            LOGGER.error("拉取消息失败", e);
        } finally {
            consumer.shutdown();
        }
    }

}

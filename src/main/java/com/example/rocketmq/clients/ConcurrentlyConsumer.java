package com.example.rocketmq.clients;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author niuzheju
 * @Date 13:59 2024/3/8
 */
public class ConcurrentlyConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentlyConsumer.class);

    public static void main(String[] args) {
        String nameServer = "192.168.56.101:9876";
        String topic = "BATCH";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("MY_CONSUMER_GROUP");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setNamesrvAddr(nameServer);
        // 消费模式, 默认集群消费
        consumer.setMessageModel(MessageModel.CLUSTERING);
        try {
            consumer.subscribe(topic, "*");
            // 并发消费
            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                for (MessageExt msg : msgs) {
                    String str = new String(msg.getBody());
                    int queueId = msg.getQueueId();
                    long queueOffset = msg.getQueueOffset();

                    System.out.println("queueId: " + queueId + " queueOffset: " + queueOffset + " message: " + str);
                    LOGGER.info("消费到消息, queueId: {}, queueOffset: {}, message: {}", queueId, queueOffset, str);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });
            consumer.start();
            LOGGER.info("开始消费。。。");
        } catch (MQClientException e) {
            LOGGER.error("并发消费消息发生错误", e);
        }

    }
}

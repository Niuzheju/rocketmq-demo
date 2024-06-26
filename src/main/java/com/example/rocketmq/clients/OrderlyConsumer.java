package com.example.rocketmq.clients;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author niuzheju
 * @Date 13:59 2024/3/8
 */
public class OrderlyConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderlyConsumer.class);

    public static void main(String[] args) throws MQClientException {
        String nameServer = "192.168.56.101:9876";
        String topic = "ORDERLY";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("MY_CONSUMER_GROUP");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setNamesrvAddr(nameServer);
        consumer.subscribe(topic, "*");
        // 顺序消费
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                String str = new String(msg.getBody());
                int queueId = msg.getQueueId();
                long queueOffset = msg.getQueueOffset();

                System.out.println("queueId: " + queueId + " queueOffset: " + queueOffset + " message: " + str);
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();
        LOGGER.info("开始消费。。。");
    }
}

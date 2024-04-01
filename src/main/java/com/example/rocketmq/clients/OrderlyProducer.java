package com.example.rocketmq.clients;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * @Author niuzheju
 * @Date 15:03 2024/3/18
 */
public class OrderlyProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderlyProducer.class);

    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("ORDERLY");
        String nameServer = "192.168.56.101:9876";
        String topic = "ORDERLY";
        producer.setNamesrvAddr(nameServer);
        try {
            // 使用订单号模队列数量的算法，让同一个订单号的消息发送到同一个队列，实现顺序发送消息
            producer.start();
            String[] tags = new String[]{"tagA", "tagB", "tagC", "tagD", "tagE"};
            for (int i = 0; i < 100; i++) {
                int orderId = i % 10;
                Message msg =
                        new Message(topic, tags[i % tags.length], "key" + i, ("orderly message" + orderId).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult result = producer.send(msg, (mqs, m, arg) -> {
                    Integer shard = (Integer) arg;
                    int index = shard % mqs.size();
                    return mqs.get(index);
                }, orderId);
                int queueId = result.getMessageQueue().getQueueId();
                LOGGER.info("orderId: {}, queueId: {}", orderId, queueId);
            }
            producer.shutdown();
        } catch (MQClientException | UnsupportedEncodingException | RemotingException | MQBrokerException |
                 InterruptedException e) {
            LOGGER.error("发送顺序消息出错", e);
        }

    }
}

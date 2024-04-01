package com.example.rocketmq.clients;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 批量消息
 * 一次发送多条消息，每次数据量不超过1M
 * 同一批次中的多条消息会被发送到同一个队列中
 * @Author niuzheju
 * @Date 14:17 2024/3/19
 */
public class BatchProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchProducer.class);

    public static void main(String[] args) {
        String nameServer = "192.168.56.101:9876";
        String topic = "BATCH";
        DefaultMQProducer producer = new DefaultMQProducer("ORDER");
        producer.setNamesrvAddr(nameServer);
        try {
            producer.start();
            List<Message> msgList = new ArrayList<>(50);
            for (int i = 0; i < 100; i++) {
                Message message = new Message(topic, "order", "order" + i, ("订单编号" + i).getBytes());
                msgList.add(message);
            }
            SendResult result = producer.send(msgList);
            LOGGER.info("返回结果, result: {}", result);
            producer.shutdown();
        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
            LOGGER.error("批量消息发送失败", e);
        }


    }
}

package com.example.rocketmq.clients;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 延迟消息
 * @Author niuzheju
 * @Date 14:12 2024/3/7
 */
public class DelayProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelayProducer.class);

    public static void main(String[] args) {
        String nameServer = "192.168.56.101:9876";
        String topic = "DELAY";
        DefaultMQProducer producer = new DefaultMQProducer("ORDER");
        producer.setNamesrvAddr(nameServer);
        try {
            producer.start();
            for (int i = 0; i < 100; i++) {
                Message message = new Message(topic, ("这是一条延迟消息, 你TM收到了吗？" + i).getBytes());
                // 4级30s
                message.setDelayTimeLevel(4);
                SendResult result = producer.send(message);
                String msgId = result.getMsgId();
                SendStatus sendStatus = result.getSendStatus();
                MessageQueue messageQueue = result.getMessageQueue();
                int queueId = messageQueue.getQueueId();
                LOGGER.info("返回结果, msgId: {}, sendStatus: {}, queueId: {}", msgId, sendStatus, queueId);
            }
            producer.shutdown();
        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
            LOGGER.error("延迟消息发送失败", e);
        }


    }
}

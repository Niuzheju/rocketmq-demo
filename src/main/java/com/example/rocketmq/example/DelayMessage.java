package com.example.rocketmq.example;

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
 * @Author niuzheju
 * @Date 14:12 2024/3/7
 */
public class DelayMessage {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelayMessage.class);

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        String nameServer = "192.168.56.101:9876";
        String topic = "ORDERLY";
        DefaultMQProducer producer = new DefaultMQProducer("MY_GROUP");
        producer.setNamesrvAddr(nameServer);
        producer.start();
        // 发送100条消息，每条消息具体发送到topic的哪个队列里是随机的
        for (int i = 0; i < 100; i++) {
            Message message = new Message(topic, ("这是一条测试消息, 你TM收到了吗？" + i).getBytes());
            SendResult result = producer.send(message);
            String msgId = result.getMsgId();
            SendStatus sendStatus = result.getSendStatus();
            MessageQueue messageQueue = result.getMessageQueue();
            int queueId = messageQueue.getQueueId();
            String brokerName = messageQueue.getBrokerName();
            String queueTopic = messageQueue.getTopic();
            LOGGER.info("返回结果, msgId: {}, sendStatus: {}, queueId: {}, brokerName: {}, queueTopic: {}", msgId, sendStatus, queueId, brokerName, queueTopic);
        }
        producer.shutdown();


    }
}

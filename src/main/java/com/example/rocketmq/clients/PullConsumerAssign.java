package com.example.rocketmq.clients;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * PULL模式消费
 * ASSIGN模式
 * 参考: <a href="https://rocketmq.apache.org/zh/docs/4.x/consumer/03pull">PULL消费</a>
 *
 * @Author niuzheju
 * @Date 14:30 2024/3/20
 */
public class PullConsumerAssign {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullConsumerAssign.class);

    private static volatile boolean running = true;

    public static void main(String[] args) {
        String nameServer = "192.168.56.101:9876";
        String topic = "PULL";
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("PULL_GROUP");
        consumer.setNamesrvAddr(nameServer);
        consumer.setAutoCommit(false);
        try {
            consumer.start();
            Collection<MessageQueue> mqSet = consumer.fetchMessageQueues(topic);
            List<MessageQueue> list = new ArrayList<>(mqSet);
            List<MessageQueue> assignList = new ArrayList<>();
            // 取其中前两个进行消费, 此处数据来源于mqSet是无序的，不一定是mq服务器topic中的前两个队列
            for (int i = 0; i < list.size() / 2; i++) {
                assignList.add(list.get(i));
            }
            consumer.assign(assignList);
            // 把第一个队列的offset设置为10, 即从10开始消费
            consumer.seek(assignList.get(0), 10);

            while (running) {
                List<MessageExt> messageExtList = consumer.poll();
                LOGGER.info("messageExtList: {}", messageExtList);
                consumer.commit();
            }
        } catch (MQClientException e) {
            LOGGER.error("拉取消息失败", e);
        } finally {
            consumer.shutdown();
        }
    }


}

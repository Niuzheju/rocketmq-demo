package com.example.rocketmq.clients;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author niuzheju
 * @Date 14:58 2024/3/19
 */
public class TransactionProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionProducer.class);

    public static void main(String[] args) {
        String nameServer = "192.168.56.101:9876";
        String topic = "TRANSACTION";
        TransactionListener transactionListener = new TransactionListenerImpl();
        TransactionMQProducer producer = new TransactionMQProducer("TRANSACTION_GROUP");
        producer.setNamesrvAddr(nameServer);
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });

        producer.setExecutorService(executorService);
        producer.setTransactionListener(transactionListener);
        try {
            producer.start();
            String[] tags = new String[]{"tagA", "tagB", "tagC", "tagD", "tagE"};
            for (int i = 0; i < 10; i++) {
                Message msg = new Message(topic, tags[i % tags.length], "key" + i, ("事务消息" + i).getBytes());
                TransactionSendResult result = producer.sendMessageInTransaction(msg, null);
                LOGGER.info("事务消息发送结果, result: {}", result);
            }
            TimeUnit.SECONDS.sleep(120);
        } catch (MQClientException e) {
            LOGGER.error("发送事务消息失败", e);
        } catch (InterruptedException e) {
            LOGGER.error("休眠线程中断", e);
        } finally {
            producer.shutdown();
        }


    }

    static class TransactionListenerImpl implements TransactionListener {

        private static final Logger LOGGER = LoggerFactory.getLogger(TransactionListenerImpl.class);

        private final AtomicInteger transIndex = new AtomicInteger();
        private final ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();


        /**
         * 执行本地事务, 发送消息后，broker储存消息成功才会调用此方法
         *
         * @param msg Half(prepare) message 半消息
         * @param arg Custom business parameter 业务参数, 发送消息时传入，这里才会有值
         * @return 事务状态
         */
        @Override
        public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
            // 此处为实验，每发送成功一条消息本方法都会被调用，这里只会让部分消息的状态更新为提交, 实际操作中应以实际业务逻辑为准
            int value = transIndex.getAndIncrement();
            int status = value % 3;
            localTrans.put(msg.getTransactionId(), status);
            LOGGER.info("执行本地事务, value: {}, localTrans: {}", value, localTrans);
            return LocalTransactionState.UNKNOW;
        }

        /**
         * 检查本地事务, executeLocalTransaction方法中返回状态为LocalTransactionState.UNKNOW时broker会调用此方法确认事务状态
         *
         * @param msg 消息内容
         * @return 事务状态
         */
        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt msg) {
            Integer status = localTrans.get(msg.getTransactionId());
            LOGGER.info("检查本地事务, status: {}", status);
            if (status != null) {
                return switch (status) {
                    case 0 -> LocalTransactionState.UNKNOW;
                    case 2 -> LocalTransactionState.ROLLBACK_MESSAGE;
                    default -> LocalTransactionState.COMMIT_MESSAGE;
                };
            }
            return LocalTransactionState.COMMIT_MESSAGE;
        }
    }
}

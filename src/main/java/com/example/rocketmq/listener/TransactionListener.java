package com.example.rocketmq.listener;

import com.example.rocketmq.service.SysLogInfoService;
import com.example.rocketmq.service.SysUserInfoService;
import lombok.Setter;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;

/**
 * 详细流程查看
 * <a href="https://rocketmq.apache.org/zh/docs/4.x/producer/06message5">事务消息发送</a>
 *
 * @Author niuzheju
 * @Date 14:53 2024/4/9
 */
@RocketMQTransactionListener
public class TransactionListener implements RocketMQLocalTransactionListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionListener.class);

    @Setter(onMethod_ = @Autowired)
    private SysUserInfoService sysUserInfoService;

    @Setter(onMethod_ = @Autowired)
    private SysLogInfoService sysLogInfoService;

    /**
     * 执行本地事务
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        try {
            LOGGER.info("arg: {}", arg);
            String key = msg.getHeaders().get("rocketmq_KEYS").toString();
            LOGGER.info("事务消息key为: {}", key);
            String message = new String((byte[]) msg.getPayload());
            LOGGER.info("事务消息message为: {}", message);

            // 执行数据库事务
            sysUserInfoService.saveUser();
        } catch (Exception e) {
            LOGGER.error("执行本地事务出现异常", e);
            return RocketMQLocalTransactionState.ROLLBACK;
        }
        return RocketMQLocalTransactionState.COMMIT;
    }

    /**
     * 检查本地事务
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
        LOGGER.info("检查本地事务");
        String key = msg.getHeaders().get(RocketMQHeaders.KEYS).toString();
        LOGGER.info("事务消息key为: {}", key);
        String message = new String((byte[]) msg.getPayload());
        LOGGER.info("事务消息message为: {}", message);

        String info = sysLogInfoService.selectLogInfo();
        if (info == null) {
            return RocketMQLocalTransactionState.ROLLBACK;
        }
        return RocketMQLocalTransactionState.COMMIT;
    }
}

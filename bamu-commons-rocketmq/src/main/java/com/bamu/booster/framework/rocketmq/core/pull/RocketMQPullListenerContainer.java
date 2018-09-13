package com.bamu.booster.framework.rocketmq.core.pull;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.exception.MQClientException;
import org.springframework.beans.factory.DisposableBean;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by @author zhuYongMing on 2018/9/3.
 */
@Slf4j
public class RocketMQPullListenerContainer implements DisposableBean {

    private AtomicBoolean started = new AtomicBoolean(false);

    @Setter
    private MQPullConsumerScheduleService pullConsumerScheduleService;


    public synchronized void start() throws MQClientException {
        if (started.get()) {
            throw new IllegalStateException("container already started. " + this.toString());
        }

        pullConsumerScheduleService.start();
        started.getAndSet(true);
    }

    public boolean isStarted() {
        return started.get();
    }

    @Override
    public void destroy() {
        started.getAndSet(false);
        if (Objects.nonNull(pullConsumerScheduleService)) {
            pullConsumerScheduleService.shutdown();
        }
    }

}

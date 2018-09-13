package com.bamu.booster.framework.rocketmq.annotation;

import com.qianbao.booster.framework.Placeholders;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface RocketMQPullTaskCallbackListener {

    /**
     * Topic name
     */
    String topic();

    /**
     * Consumers of the same role is required to have exactly same subscriptions and consumerGroup to correctly achieve
     * load balance. It's required and needs to be globally unique.
     * </p>
     * <p>
     * See <a href="http://rocketmq.apache.org/docs/core-concept/">here</a> for further discussion.
     */
    String consumerGroup() default Placeholders.APPLICATION_NAME + "_consumer";

    /**
     * pull scheduled ThreadPoolExecutor thread numbers
     */
    int pullThreadNumbers() default 20;
}

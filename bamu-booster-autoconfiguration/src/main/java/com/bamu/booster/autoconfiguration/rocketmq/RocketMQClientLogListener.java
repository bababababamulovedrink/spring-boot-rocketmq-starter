package com.bamu.booster.autoconfiguration.rocketmq;

import com.qianbao.booster.boot.EnvironmentUtil;
import com.qianbao.booster.core.util.ClassUtil;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.boot.logging.LoggingApplicationListener;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;

/**
 * @author zhuyongming
 * @since 2018/7/5
 */
public class RocketMQClientLogListener implements ApplicationListener<ApplicationEnvironmentPreparedEvent>, Ordered {

    @Override
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
        // don't listen to events in a spring cloud context
        if (EnvironmentUtil.isSpringCloudContext(event.getEnvironment())) {
            return;
        }

        ClassLoader classLoader = event.getSpringApplication().getClassLoader();
        if (ClassUtil.isPresent("org.apache.rocketmq.client.log.ClientLogger", classLoader)) {
            Logger log = LoggerFactory.getLogger(LoggerName.CLIENT_LOGGER_NAME);
            ClientLogger.setLog(log);
        }
    }

    @Override
    public int getOrder() {
        return LoggingApplicationListener.DEFAULT_ORDER + 10;
    }
}

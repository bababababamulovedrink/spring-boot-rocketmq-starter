/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bamu.booster.autoconfiguration.rocketmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qianbao.booster.core.InternalException;
import com.qianbao.booster.framework.rocketmq.annotation.RocketMQListener;
import com.qianbao.booster.framework.rocketmq.annotation.RocketMQPullTaskCallbackListener;
import com.qianbao.booster.framework.rocketmq.core.RocketMQConsumer;
import com.qianbao.booster.framework.rocketmq.core.RocketMQTemplate;
import com.qianbao.booster.framework.rocketmq.core.pull.RocketMQPullListenerContainer;
import com.qianbao.booster.framework.rocketmq.core.push.RocketMQPushListenerContainer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.consumer.PullTaskCallback;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.qianbao.booster.autoconfigure.rocketmq.DefaultRocketMQListenerContainerConstants.*;

@Configuration
@EnableConfigurationProperties(RocketMQProperties.class)
@ConditionalOnClass({MQClientAPIImpl.class, RocketMQTemplate.class})
@Slf4j
@RequiredArgsConstructor
public class RocketMQAutoConfiguration {

    private final RocketMQProperties rocketMQProperties;

    /*---------------------------------生产者配置---------------------------------*/

    @Bean
    @ConditionalOnClass(name = "org.apache.rocketmq.client.producer.DefaultMQProducer")
    @ConditionalOnMissingBean(DefaultMQProducer.class)
    @ConditionalOnProperty(prefix = "spring.rocketmq", value = {"nameServer", "producer.group"})
    public DefaultMQProducer mqProducer(RocketMQProperties rocketMQProperties) {

        RocketMQProperties.Producer producerConfig = rocketMQProperties.getProducer();
        String groupName = producerConfig.getGroup();
        Assert.hasText(groupName, "[spring.rocketmq.producer.group] must not be null");

        DefaultMQProducer producer = new DefaultMQProducer(producerConfig.getGroup());
        producer.setNamesrvAddr(rocketMQProperties.getNameServer());
        producer.setSendMsgTimeout(producerConfig.getSendMsgTimeout());
        producer.setRetryTimesWhenSendFailed(producerConfig.getRetryTimesWhenSendFailed());
        producer.setRetryTimesWhenSendAsyncFailed(producerConfig.getRetryTimesWhenSendAsyncFailed());
        producer.setMaxMessageSize(producerConfig.getMaxMessageSize());
        producer.setCompressMsgBodyOverHowmuch(producerConfig.getCompressMsgBodyOverHowmuch());
        producer.setRetryAnotherBrokerWhenNotStoreOK(producerConfig.isRetryAnotherBrokerWhenNotStoreOk());

        return producer;
    }

    @Bean(destroyMethod = "destroy")
    @ConditionalOnBean(DefaultMQProducer.class)
    @ConditionalOnMissingBean(RocketMQTemplate.class)
    public RocketMQTemplate rocketMQTemplate(DefaultMQProducer mqProducer, ObjectProvider<ObjectMapper> objectMapper)
            throws MQClientException {
        RocketMQTemplate rocketMQTemplate = new RocketMQTemplate();
        rocketMQTemplate.setProducer(mqProducer);
        Optional.ofNullable(objectMapper.getIfAvailable()).ifPresent(rocketMQTemplate::setObjectMapper);
        if (rocketMQProperties.getEnabled()) {
            rocketMQTemplate.start();
        }
        return rocketMQTemplate;
    }

    /*---------------------------------消费者配置---------------------------------*/

    @Configuration
    @ConditionalOnClass(name = {
            "org.apache.rocketmq.client.consumer.DefaultMQPushConsumer",
            "org.apache.rocketmq.client.consumer.DefaultMQPullConsumer"}
    )
    @ConditionalOnProperty(prefix = "spring.rocketmq", value = "nameServer")
    @Order
    public static class ListenerContainerConfiguration implements ApplicationContextAware, InitializingBean {
        private ConfigurableApplicationContext applicationContext;

        private AtomicLong pushContainerCounter = new AtomicLong(0);

        private AtomicLong pullContainerCounter = new AtomicLong(0);

        @Resource
        private StandardEnvironment environment;

        @Resource
        private RocketMQProperties rocketMQProperties;

        private ObjectMapper objectMapper;

        public ListenerContainerConfiguration() {

        }

        @Autowired(required = false)
        public ListenerContainerConfiguration(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
            this.applicationContext = (ConfigurableApplicationContext) applicationContext;
        }

        @Override
        public void afterPropertiesSet() {
            if (rocketMQProperties.getEnabled()) {
                Map<String, Object> pushConsumers = this.applicationContext.getBeansWithAnnotation(RocketMQListener.class);
                if (Objects.nonNull(pushConsumers)) {
                    pushConsumers.forEach(this::registerPushContainer);
                }

                Map<String, Object> pullTasks = this.applicationContext.getBeansWithAnnotation(RocketMQPullTaskCallbackListener.class);
                if (Objects.nonNull(pullTasks)) {
                    pullTasks.forEach(this::registerPullContainer);
                }
            }
        }

        private void registerPushContainer(String beanName, Object bean) {
            Class<?> clazz = AopUtils.getTargetClass(bean);

            if (!RocketMQConsumer.class.isAssignableFrom(bean.getClass())) {
                throw new IllegalStateException(clazz + " is not instance of " + RocketMQConsumer.class.getName());
            }

            RocketMQConsumer rocketMQConsumer = (RocketMQConsumer) bean;
            RocketMQListener annotation = clazz.getAnnotation(RocketMQListener.class);
            BeanDefinitionBuilder beanBuilder = BeanDefinitionBuilder.rootBeanDefinition(RocketMQPushListenerContainer.class);
            beanBuilder.addPropertyValue(PROP_NAMESERVER, rocketMQProperties.getNameServer());
            beanBuilder.addPropertyValue(PROP_TOPIC, environment.resolvePlaceholders(annotation.topic()));

            beanBuilder.addPropertyValue(PROP_CONSUMER_GROUP, environment.resolvePlaceholders(annotation.consumerGroup()));
            beanBuilder.addPropertyValue(PROP_CONSUME_MODE, annotation.consumeMode());
            beanBuilder.addPropertyValue(PROP_CONSUME_THREAD_MAX, annotation.consumeThreadMax());
            beanBuilder.addPropertyValue(PROP_MESSAGE_MODEL, annotation.messageModel());
            beanBuilder.addPropertyValue(PROP_SELECTOR_EXPRESS, environment.resolvePlaceholders(annotation.selectorExpress()));
            beanBuilder.addPropertyValue(PROP_SELECTOR_TYPE, annotation.selectorType());
            beanBuilder.addPropertyValue(PROP_ROCKETMQ_CONSUMER, rocketMQConsumer);
            if (Objects.nonNull(objectMapper)) {
                beanBuilder.addPropertyValue(PROP_OBJECT_MAPPER, objectMapper);
            }
            beanBuilder.setDestroyMethodName(METHOD_DESTROY);

            String containerBeanName = String.format("%s_%s", RocketMQPushListenerContainer.class.getName(), pushContainerCounter.incrementAndGet());
            DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) applicationContext.getBeanFactory();
            beanFactory.registerBeanDefinition(containerBeanName, beanBuilder.getBeanDefinition());

            RocketMQPushListenerContainer container = beanFactory.getBean(containerBeanName, RocketMQPushListenerContainer.class);

            if (!container.isStarted()) {
                try {
                    container.start();
                } catch (Exception e) {
                    log.error("started container failed. {}", container, e);
                    throw new InternalException(e);
                }
            }

            log.info("register rocketMQ listener to container, listenerBeanName:{}, containerBeanName:{}", beanName, containerBeanName);
        }

        private void registerPullContainer(String beanName, Object bean) {
            Class<?> clazz = AopUtils.getTargetClass(bean);
            if (!PullTaskCallback.class.isAssignableFrom(bean.getClass())) {
                throw new IllegalStateException(clazz + " is not instance of " + PullTaskCallback.class.getName());
            }

            PullTaskCallback callback = (PullTaskCallback) bean;
            RocketMQPullTaskCallbackListener annotation = clazz.getAnnotation(RocketMQPullTaskCallbackListener.class);

            String consumerGroup = annotation.consumerGroup();
            MQPullConsumerScheduleService consumerScheduleService =
                    new MQPullConsumerScheduleService(environment.resolvePlaceholders(consumerGroup));
            consumerScheduleService.setPullThreadNums(annotation.pullThreadNumbers());
            consumerScheduleService.registerPullTaskCallback(environment.resolvePlaceholders(annotation.topic()), callback);

            BeanDefinitionBuilder beanBuilder = BeanDefinitionBuilder.rootBeanDefinition(RocketMQPullListenerContainer.class);
            beanBuilder.addPropertyValue(PULL_CONSUMER_SCHEDULE_SERVICE, consumerScheduleService);
            beanBuilder.setDestroyMethodName(METHOD_DESTROY);

            String containerBeanName = String.format("%s_%s", RocketMQPullListenerContainer.class.getName(), pullContainerCounter.incrementAndGet());
            DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) applicationContext.getBeanFactory();
            beanFactory.registerBeanDefinition(containerBeanName, beanBuilder.getBeanDefinition());

            RocketMQPullListenerContainer container = beanFactory.getBean(containerBeanName, RocketMQPullListenerContainer.class);
            if (!container.isStarted()) {
                try {
                    container.start();
                } catch (Exception e) {
                    log.error("started RocketMQPullListenerContainer failed. consumer group : {}", consumerGroup, e);
                    throw new InternalException(e);
                }
            }

            log.info("register RocketMQPullListenerContainer success listenerBeanName:{}, consumerGroupName:{}", beanName, consumerGroup);
        }
    }
}

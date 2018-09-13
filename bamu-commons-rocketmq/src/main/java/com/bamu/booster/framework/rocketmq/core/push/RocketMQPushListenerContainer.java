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

package com.bamu.booster.framework.rocketmq.core.push;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.qianbao.booster.core.util.ClassUtil;
import com.qianbao.booster.framework.rocketmq.core.Message;
import com.qianbao.booster.framework.rocketmq.core.RocketMQConsumer;
import com.qianbao.booster.framework.rocketmq.core.RocketMQExtConsumer;
import com.qianbao.booster.framework.rocketmq.enums.ConsumeMode;
import com.qianbao.booster.framework.rocketmq.enums.SelectorType;
import com.qianbao.booster.jackson.utility.JSON;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("WeakerAccess")
@Slf4j
public class RocketMQPushListenerContainer implements InitializingBean, RocketMQPushConsumerContainer {

    @Setter
    @Getter
    private long suspendCurrentQueueTimeMillis = 1000;

    /**
     * Message consume retry strategy<br> -1,no retry,put into DLQ directly<br> 0,broker control retry frequency<br>
     * >0,client control retry frequency
     */
    @Setter
    @Getter
    private int delayLevelWhenNextConsume = 0;

    @Setter
    @Getter
    private String consumerGroup;

    @Setter
    @Getter
    private String nameServer;

    @Setter
    @Getter
    private String topic;

    @Setter
    @Getter
    private ConsumeMode consumeMode = ConsumeMode.CONCURRENTLY;

    @Setter
    @Getter
    private SelectorType selectorType = SelectorType.TAG;

    @Setter
    @Getter
    private String selectorExpress = "*";

    @Setter
    @Getter
    private MessageModel messageModel = MessageModel.CLUSTERING;

    @Setter
    @Getter
    private int consumeThreadMax = 64;

    @Getter
    @Setter
    private String charset = "UTF-8";

    @Setter
    @Getter
    private ObjectMapper objectMapper = JSON.objectMapper();

    @Setter
    @Getter
    private boolean started;

    @Setter
    private RocketMQConsumer<?> rocketMQConsumer;

    private DefaultMQPushConsumer consumer;

    private JavaType messageType;

    @Override
    public void setupMessageConsumer(RocketMQConsumer<?> rocketMQConsumer) {
        this.rocketMQConsumer = rocketMQConsumer;
    }

    @Override
    public void destroy() {
        this.setStarted(false);
        if (Objects.nonNull(consumer)) {
            consumer.shutdown();
        }
        log.info("container destroyed, {}", this.toString());
    }

    public synchronized void start() throws MQClientException {

        if (this.isStarted()) {
            throw new IllegalStateException("container already started. " + this.toString());
        }

        initRocketMQPushConsumer();

        // parse message type
        this.messageType = getMessageType();
        log.debug("msgType: {}", messageType.toString());

        consumer.start();
        this.setStarted(true);

        log.info("started container: {}", this.toString());
    }

    @Override
    public String toString() {
        return "RocketMQPushListenerContainer{" +
                "consumerGroup='" + consumerGroup + '\'' +
                ", nameServer='" + nameServer + '\'' +
                ", topic='" + topic + '\'' +
                ", consumeMode=" + consumeMode +
                ", selectorType=" + selectorType +
                ", selectorExpress='" + selectorExpress + '\'' +
                ", messageModel=" + messageModel +
                '}';
    }

    private JavaType getMessageType() {
        Type[] interfaces = rocketMQConsumer.getClass().getGenericInterfaces();
        TypeFactory typeFactory = objectMapper.getTypeFactory();
        if (Objects.nonNull(interfaces)) {
            for (Type type : interfaces) {
                if (type instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) type;
                    Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                    if (actualTypeArguments == null || actualTypeArguments.length == 0) {
                        return typeFactory.constructType(Object.class);
                    }
                    Type messageType = actualTypeArguments[0];
                    if (Objects.equals(parameterizedType.getRawType(), RocketMQConsumer.class)) {
                        return typeFactory.constructType(messageType);
                    }
                    if (Objects.equals(parameterizedType.getRawType(), RocketMQExtConsumer.class)) {
                        return typeFactory.constructParametricType(Message.class, typeFactory.constructType(messageType));
                    }
                }
            }
            return typeFactory.constructType(Object.class);
        } else {
            return typeFactory.constructType(Object.class);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        start();
    }

    private void initRocketMQPushConsumer() throws MQClientException {

        Assert.notNull(rocketMQConsumer, "Property 'rocketMQListener' is required");
        Assert.notNull(consumerGroup, "Property 'consumerGroup' is required");
        Assert.notNull(nameServer, "Property 'nameServer' is required");
        Assert.notNull(topic, "Property 'topic' is required");

        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(nameServer);
        consumer.setConsumeThreadMax(consumeThreadMax);
        if (consumeThreadMax < consumer.getConsumeThreadMin()) {
            consumer.setConsumeThreadMin(consumeThreadMax);
        }

        consumer.setMessageModel(messageModel);

        switch (selectorType) {
            case TAG:
                consumer.subscribe(topic, selectorExpress);
                break;
            case SQL92:
                consumer.subscribe(topic, MessageSelector.bySql(selectorExpress));
                break;
            default:
                throw new IllegalArgumentException("Property 'selectorType' was wrong.");
        }

        switch (consumeMode) {
            case ORDERLY:
                consumer.setMessageListener(new DefaultMessageListenerOrderly());
                break;
            case CONCURRENTLY:
                consumer.setMessageListener(new DefaultMessageListenerConcurrently());
                break;
            default:
                throw new IllegalArgumentException("Property 'consumeMode' was wrong.");
        }

        // provide an entryway to custom setting RocketMQ consumer
        if (rocketMQConsumer instanceof RocketMQPushConsumerLifecycleListener) {
            ((RocketMQPushConsumerLifecycleListener) rocketMQConsumer).prepareStart(consumer);
        }

    }

    @SuppressWarnings("unchecked")
    private Object doConvertMessage(MessageExt messageExt) {
        if (Objects.equals(messageType.getRawClass(), MessageExt.class)) {
            return messageExt;
        }
        if (Objects.equals(messageType.getRawClass(), Message.class)) {
            Object body = parseBody(messageExt, messageType.getBindings().getBoundType(0));
            return new Message<>(messageExt, body);
        } else {
            return parseBody(messageExt, messageType);
        }
    }

    private Object parseBody(MessageExt messageExt, JavaType bodyType) {
        String str = new String(messageExt.getBody(), Charset.forName(charset));
        if (Objects.equals(bodyType.getRawClass(), String.class)) {
            return str;
        } else {
            // if msgType not string, use objectMapper change it.
            try {
                return objectMapper.readValue(str, bodyType);
            } catch (Exception e) {
                log.info("convert failed. str:{}, msgType:{}", str, messageType);
                throw new RuntimeException("cannot convert message to " + messageType, e);
            }
        }
    }

    public class DefaultMessageListenerConcurrently implements MessageListenerConcurrently {

        @SuppressWarnings("unchecked")
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            for (MessageExt messageExt : msgs) {
                log.debug("received msg: {}", messageExt);
                try {
                    long now = System.currentTimeMillis();
                    rocketMQConsumer.onMessage(ClassUtil.cast(doConvertMessage(messageExt)));
                    long costTime = System.currentTimeMillis() - now;
                    log.debug("consume {} cost: {} ms", messageExt.getMsgId(), costTime);
                } catch (Exception e) {
                    log.warn("consume message failed. messageExt:{}", messageExt, e);
                    context.setDelayLevelWhenNextConsume(delayLevelWhenNextConsume);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

    public class DefaultMessageListenerOrderly implements MessageListenerOrderly {

        @SuppressWarnings("unchecked")
        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            for (MessageExt messageExt : msgs) {
                log.debug("received msg: {}", messageExt);
                try {
                    long now = System.currentTimeMillis();
                    rocketMQConsumer.onMessage(ClassUtil.cast(doConvertMessage(messageExt)));
                    long costTime = System.currentTimeMillis() - now;
                    log.info("consume {} cost: {} ms", messageExt.getMsgId(), costTime);
                } catch (Exception e) {
                    log.warn("consume message failed. messageExt:{}", messageExt, e);
                    context.setSuspendCurrentQueueTimeMillis(suspendCurrentQueueTimeMillis);
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
            }

            return ConsumeOrderlyStatus.SUCCESS;
        }
    }

}

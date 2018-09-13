package com.bamu.booster.framework.rocketmq.core.pull;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.qianbao.booster.jackson.utility.JSON;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullTaskCallback;
import org.apache.rocketmq.client.consumer.PullTaskContext;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;

/**
 * Created by @author zhuYongMing on 2018/9/4.
 */
public abstract class CustomPullTaskCallback<T> implements PullTaskCallback {


    private ObjectMapper objectMapper = JSON.objectMapper();

    private static final int SIZE = 1000;

    @Override
    public void doPullTask(MessageQueue mq, PullTaskContext context) {
        MQPullConsumer consumer = context.getPullConsumer();
        try {
            long offset = consumer.fetchConsumeOffset(mq, true);
            PullResult result = consumer.pullBlockIfNotFound(mq, null, offset, SIZE);
            List<MessageExt> foundList = result.getMsgFoundList();





            long nextOffset = result.getNextBeginOffset();
            if (foundList.size() < SIZE) {
                nextOffset = offset + foundList.size();
            }
            consumer.updateConsumeOffset(mq, nextOffset);
        } catch (MQClientException | InterruptedException | RemotingException | MQBrokerException e) {
            System.out.println("发生异常。。。。。。");
        }
    }

    private List<T> parseMessage(List<MessageExt> messageExts) {

    }

    private JavaType getMessageType() {
        Type[] interfaces = this.getClass().getGenericInterfaces();
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
                    if (Objects.equals(parameterizedType.getRawType(), CustomPullTaskCallback.class)) {
                        return typeFactory.constructType(messageType);
                    }
                    /*if (Objects.equals(parameterizedType.getRawType(), RocketMQExtConsumer.class)) {
                        return typeFactory.constructParametricType(Message.class, typeFactory.constructType(messageType));
                    }*/
                }
            }
            return typeFactory.constructType(Object.class);
        } else {
            return typeFactory.constructType(Object.class);
        }
    }


    abstract void doPullTask(List<T> messages);
}

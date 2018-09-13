package com.bamu.booster.framework.rocketmq.core;

import lombok.Data;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * A message representation with headers and body.
 *
 * @author zhuyongming
 * @since 2018/7/5
 */
@Data
public class Message<T> {

    private final MessageExt ext;

    private final T body;
}

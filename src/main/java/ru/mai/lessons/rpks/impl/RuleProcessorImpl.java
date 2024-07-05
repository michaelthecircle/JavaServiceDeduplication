package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RedisClient;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {

    private final RedisClient redisClient;

    public RuleProcessorImpl(Config config){
        redisClient = new RedisClientImpl(config);
    }

    @Override
    public Message processing(Message message, Rule[] rules) {
        if (message.getValue() == null || message.getValue().isEmpty()) {
            message.setDeduplicationState(false);
            return message;
        }

        if (rules == null || rules.length == 0){
            message.setDeduplicationState(true);
            return message;
        }

        message.setDeduplicationState(redisClient.filterMessage(message, rules));
        return message;
    }
}
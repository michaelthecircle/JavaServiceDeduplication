package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;

import ru.mai.lessons.rpks.RedisClient;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.MessageStateExpire;
import ru.mai.lessons.rpks.model.Rule;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RedisClientImpl implements RedisClient {

    private final JedisPool jedisPool;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RedisClientImpl(Config config) {
        this.jedisPool = new JedisPool(config.getString("redis.host"), config.getInt("redis.port"));
    }

    @Override
    public boolean filterMessage(Message message, Rule[] rules) {
        MessageStateExpire messageStateExpire = generateMessageStateExpire(message, rules);
        if (messageStateExpire == null) {
            return false;
        }

        String key = messageStateExpire.getMessageKey();
        try (Jedis jedis = jedisPool.getResource()) {
            if (jedis.exists(key)) {
                return false;
            }

            if (key != null && !key.isEmpty()) {
                jedis.set(key, message.getValue());
                jedis.expire(key, messageStateExpire.getExpireTime());
            }
        } catch (Exception e) {
            log.error("Error processing message in RedisClientImpl: {}", e.getMessage(), e);
            return false;
        }
        return true;
    }

    private MessageStateExpire generateMessageStateExpire(Message message, Rule[] rules) {
        List<String> keyBuilder = new ArrayList<>();
        MessageStateExpire stateExpire = new MessageStateExpire();

        try {
            ObjectNode node = objectMapper.readValue(message.getValue(), ObjectNode.class);

            for (Rule rule : rules) {
                if (Boolean.TRUE.equals(rule.getIsActive()) && node.has(rule.getFieldName())) {
                    keyBuilder.add(node.get(rule.getFieldName()).asText());
                    if (stateExpire.getExpireTime() < rule.getTimeToLiveSec()) {
                        stateExpire.setExpireTime(rule.getTimeToLiveSec());
                    }
                }
            }
            stateExpire.setMessageKey(String.join(":", keyBuilder));
            return stateExpire;

        } catch (Exception e) {
            log.error("JSON exception occurred in RedisClientImpl: {}", e.getMessage(), e);
            return null;
        }
    }
}

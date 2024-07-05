package ru.mai.lessons.rpks.model;

import lombok.Data;


@Data
public class MessageStateExpire {
    private long expireTime;
    private String messageKey;
}

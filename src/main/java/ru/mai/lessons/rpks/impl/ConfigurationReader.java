package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.ConfigReader;

@Slf4j
public class ConfigurationReader implements ConfigReader {
    @Override
    public Config loadConfig() {
        Config config = ConfigFactory.parseResources("application.conf");
        log.info("config from 'application.conf' was loaded successfully");
        return config;
    }
}

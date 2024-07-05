package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;

@Slf4j
public class DbReaderImpl implements DbReader {
    private final HikariDataSource dataSource;
    
    public DbReaderImpl(Config config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
        hikariConfig.setUsername(config.getString("db.user"));
        hikariConfig.setPassword(config.getString("db.password"));
        hikariConfig.setDriverClassName(config.getString("db.driver"));
        this.dataSource =  new HikariDataSource(hikariConfig);
    }
    
    @Override
    public Rule[] readRulesFromDB() {
        try (var connection = dataSource.getConnection()) {
            DSLContext dslContext = DSL.using(connection, SQLDialect.POSTGRES);
            var result = dslContext.select().from("deduplication_rules").fetch();
            Rule[] rules = new Rule[result.size()];
            
            for (int index = 0; index < result.size(); index++) {
                rules[index] = Rule.builder()
                        .deduplicationId(result.get(index).getValue("deduplication_id", Long.class))
                        .ruleId(result.get(index).getValue("rule_id", Long.class))
                        .fieldName(result.get(index).getValue("field_name", String.class))
                        .timeToLiveSec(result.get(index).getValue("time_to_live_sec", Long.class))
                        .isActive(result.get(index).getValue("is_active", Boolean.class))
                        .build();
            }
            return rules;
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        return new Rule[0];
    }
}
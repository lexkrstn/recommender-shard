package com.lexkrstn.recommender.shard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(ApplicationConfig.class)
public class ShardApplication {
    public static void main(String[] args) {
        SpringApplication.run(ShardApplication.class, args);
    }

}

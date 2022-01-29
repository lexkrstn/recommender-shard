package com.lexkrstn.recommender.shard;

import com.lexkrstn.recommender.shard.io.PreferenceFile;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
@ConfigurationProperties(prefix = "com.lexkrstn.recommender.shard")
@Getter
@Setter
public class ApplicationConfig {
    private final Logger log = LoggerFactory.getLogger(ApplicationConfig.class);

    private int maxRecommendTasks;
    private String dataFilePath;

    @Bean
    public RecommenderThread recommenderThread() {
        try {
            String fullPath = dataFilePath.startsWith("/")
                ? dataFilePath
                : System.getProperty("user.dir") + "/" + dataFilePath;
            return new RecommenderThread(new PreferenceFile(fullPath), maxRecommendTasks);
        } catch (IOException e) {
            log.error(e.toString());
            return null;
        }

    }
}

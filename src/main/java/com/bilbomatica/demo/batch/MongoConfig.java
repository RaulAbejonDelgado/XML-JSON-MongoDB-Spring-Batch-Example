package com.bilbomatica.demo.batch;

import java.io.IOException;
import cz.jirutka.spring.embedmongo.EmbeddedMongoFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.*;
import com.mongodb.MongoClient;
 
 
@Configuration
public class MongoConfig {
 
    private static final String MONGO_DB_URL = "localhost";
    private static final String MONGO_DB_NAME = "test_batch";
    private static final int MONGO_PORT = 27019;

    @Bean
    public MongoTemplate mongoTemplate() throws IOException {
        EmbeddedMongoFactoryBean mongo = new EmbeddedMongoFactoryBean();
        mongo.setBindIp(MONGO_DB_URL);
        mongo.setPort(MONGO_PORT);
        MongoClient mongoClient = mongo.getObject();
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, MONGO_DB_NAME);
        return mongoTemplate;
    }
}
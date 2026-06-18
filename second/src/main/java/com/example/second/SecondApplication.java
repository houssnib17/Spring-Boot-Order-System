package com.example.second;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;

@SpringBootApplication(exclude = {
        DataSourceAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class
})
@EnableAsync // المتطلب الثالث: المعالجة غير المتزامنة
public class SecondApplication {

    public static void main(String[] args) {
        SpringApplication.run(SecondApplication.class, args);
    }

    // تهيئة الـ Redis Template للتسلسل والتحويل الآمن للـ JSON لمنع أخطاء القراءة
    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        return template;
    }

    // محاكاة هيكلية نقية للـ TransactionManager لضمان سلامة عمل الـ Aspect الخاص بـ @Transactional
    @Bean
    public PlatformTransactionManager transactionManager() {
        return new PlatformTransactionManager() {
            @Override
            public TransactionStatus getTransaction(TransactionDefinition definition) throws TransactionException {
                return new TransactionStatus() {
                    @Override public boolean isNewTransaction() { return true; }
                    @Override public boolean hasSavepoint() { return false; }
                    @Override public void setRollbackOnly() {}
                    @Override public boolean isRollbackOnly() { return false; }
                    @Override public void flush() {}
                    @Override public boolean isCompleted() { return false; }
                    @Override public Object createSavepoint() throws TransactionException { return null; }
                    @Override public void rollbackToSavepoint(Object savepoint) throws TransactionException {}
                    @Override public void releaseSavepoint(Object savepoint) throws TransactionException {}
                };
            }

            @Override
            public void commit(TransactionStatus status) throws TransactionException {}

            @Override
            public void rollback(TransactionStatus status) throws TransactionException {}
        };
    }
}
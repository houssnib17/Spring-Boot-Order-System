package com.example.second.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class AsyncConfig {

    @Bean(name = "customTaskExecutor")
    public Executor customTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // التخصيص الهندسي لـ 10 مستخدمين متزامنين
        executor.setCorePoolSize(4);       // معالجة 4 طلبات فوراً كخيوط أساسية
        executor.setMaxPoolSize(4);        // سقف النظام الثابت لمنع استهلاك الموارد المفرط
        executor.setQueueCapacity(6);      // وضع الـ 6 طلبات المتبقية في الطابور (المجموع 10)

        executor.setThreadNamePrefix("EcomExecutor-");

        executor.initialize();
        return executor;
    }
}
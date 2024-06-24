package com.qing.learn.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;


/**
 * ### 总结
 *
 * 通过以上配置和代码示例，我们使用Redis的`SETNX`命令来确保Kafka消息处理的幂等性，从而防止消息重复消费。具体步骤如下：
 *
 * 1. **分布式锁**：使用Redis的`SETNX`命令确保同一时间只有一个实例能够处理特定消息，并设置锁的过期时间。
 * 2. **消息幂等性**：在处理消息前，检查Redis中是否存在已处理的标记，防止重复消费。
 * 3. **处理完成后标记**：在处理消息后，将消息ID存储到Redis中并设置过期时间，以记录该消息已经被处理过。
 *
 * 通过这种方式，可以有效地防止消息的重复消费。
 */
@Service
class KafkaConsumerService2 {

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final String MESSAGE_PROCESSED_KEY_PREFIX = "kafka:processed:";
    private static final long LOCK_EXPIRE_TIME = 5; // 锁超时时间，单位为秒

    @KafkaListener(topics = "my-topic", groupId = "my-group")
    public void listen(ConsumerRecord<String, String> record) {
        String messageId = record.key();
        String messageValue = record.value();
        String lockKey = "lock:" + messageId;
        String processedKey = MESSAGE_PROCESSED_KEY_PREFIX + messageId;

        // 检查消息是否已处理
        if (Boolean.TRUE.equals(redisTemplate.hasKey(processedKey))) {
            System.out.println("消息已处理: " + messageId);
            return;
        }

        try {
            // 尝试获取锁，使用SETNX命令并设置超时时间
            Boolean lockAcquired = redisTemplate.opsForValue().setIfAbsent(lockKey, "locked", LOCK_EXPIRE_TIME, TimeUnit.SECONDS);
            if (Boolean.TRUE.equals(lockAcquired)) {
                try {
                    // 双重检查避免竞争条件
                    if (Boolean.TRUE.equals(redisTemplate.hasKey(processedKey))) {
                        System.out.println("消息已处理: " + messageId);
                        return;
                    }

                    // 处理消息
                    processMessage(messageValue);

                    // 标记消息已处理
                    redisTemplate.opsForValue().set(processedKey, "processed", 1, TimeUnit.DAYS);
                } finally {
                    // 删除锁
                    redisTemplate.delete(lockKey);
                }
            } else {
                // 未能获取锁，可能是重复消息，忽略
                System.out.println("未能获取锁，消息可能正在被处理: " + messageId);
            }
        } catch (Exception e) {
            System.err.println("处理消息时出现异常: " + messageId);
            e.printStackTrace();
        }
    }

    private void processMessage(String message) {
        // 实际的消息处理逻辑
        System.out.println("处理消息: " + message);
    }
}
package com.zou.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author leonard
 * @date 2022/11/17
 * @Description TODO
 */
@Slf4j
@Component
public class KafkaConsumerListener {

    //    @KafkaListener(topics = "${kafka.topic.one}")
    //    public void onMessage(ConsumerRecord<String, String> record) {
    //        consumer(record);
    //    }


    @KafkaListener(topics = "my-topic",containerFactory = "batchFactory")
    public void onMessage(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        log.info("batch listen size {}.", records.size());
        try {
            records.forEach(it -> consumer(it));
        } finally {
            ack.acknowledge();  //手动提交偏移量
        }
    }

    /**
     * 单条消费
     */
    private void consumer(ConsumerRecord<String, String> record) {
        log.info("消费消息 主题:{}, 内容: {}", record.topic(), record.value());
    }


}

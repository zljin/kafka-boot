package com.zou.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

@Slf4j
@Component
public class KafkaSendUtil {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Value("${spring.kafka.bootstrap-servers}")
    private String[] servers;


    private AdminClient getAdmin() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", servers);
        return AdminClient.create(properties);
    }

    public Set<String> queryTopic() {
        AdminClient admin = getAdmin();
        ListTopicsResult listTopicsResult = admin.listTopics();
        try {
            Set<String> sets = listTopicsResult.names().get();
            return sets;
        } catch (Exception e) {
            log.error("query topic error");
        } finally {
            admin.close();
        }
        return null;
    }

    public void deleteTopic(List<String> names) {
        AdminClient admin = getAdmin();
        try {
            admin.deleteTopics(names);
        } finally {
            admin.close();
        }
    }

    public boolean addTopic(String topicName, Integer partition, Integer replica) {
        AdminClient admin = getAdmin();
        if (replica > servers.length) {
            return false;
        }
        try {
            NewTopic newTopic = new NewTopic(topicName, partition, Short.parseShort(replica.toString()));
            admin.createTopics(Collections.singletonList(newTopic));
        } finally {
            admin.close();
        }
        return true;
    }


    /**
     * 指定 topic 和 message
     *
     * @param topic 主题
     * @param data  发送内容
     */
    public void send(String topic, String data) {
        log.info("kafka Producer发送消息，topic={}, data={}", topic, data);
        ListenableFuture<SendResult<String, String>> listenableFuture = kafkaTemplate.send(topic, data);

        try {
            SendResult<String, String> sendResult = listenableFuture.get();
            listenableFuture.addCallback(SuccessCallback -> successCallback(sendResult)
                    , FailureCallback -> failureCallback(sendResult));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 指定 topic、分区 和 message
     *
     * @param topic     主题
     * @param partition 分区
     * @param data      发送内容
     */
    public void send(String topic, int partition, String data) {
        log.info("kafka Producer发送消息，topic={}", topic);
        ListenableFuture<SendResult<String, String>> listenableFuture = kafkaTemplate.send(topic, partition, null, data);

        try {
            SendResult<String, String> sendResult = listenableFuture.get();
            listenableFuture.addCallback(SuccessCallback -> successCallback(sendResult)
                    , FailureCallback -> failureCallback(sendResult));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 指定 topic、分区 和 message
     *
     * @param topic 主题
     * @param key   标志
     * @param data  发送内容
     */
    public void send(String topic, String key, String data) {
        log.info("kafka Producer发送消息，topic={},key={},data={}", topic, key, data);
        ListenableFuture<SendResult<String, String>> listenableFuture = kafkaTemplate.send(topic, key, data);

        try {
            SendResult<String, String> sendResult = listenableFuture.get();
            listenableFuture.addCallback(SuccessCallback -> successCallback(sendResult)
                    , FailureCallback -> failureCallback(sendResult));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 成功后的回调
     */
    private void successCallback(SendResult<String, String> sendResult) {
        log.info("kafka Producer发送消息成功！topic={}，partition={}，offset={}",
                sendResult.getRecordMetadata().topic(),
                sendResult.getRecordMetadata().partition(),
                sendResult.getRecordMetadata().offset());
    }

    /**
     * 失败后的回调
     */
    private void failureCallback(SendResult<String, String> sendResult) {
        log.error("kafka Producer消息发送失败！sendResult={}", sendResult.getProducerRecord());
    }


}

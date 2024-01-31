package com.zou.controller;

import com.alibaba.fastjson.JSONObject;
import com.zou.config.KafkaSendUtil;
import com.zou.pojo.DevMachine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    KafkaSendUtil kafkaSendUtil;

    @Value("${kafka.topic.one}")
    String myTopic;

    @GetMapping("/prod")
    public ResponseEntity prod() {
        kafkaSendUtil.send(myTopic, JSONObject.toJSONString(prodTestData()));
        return ResponseEntity.ok("");
    }

    private DevMachine prodTestData() {
        DevMachine devMachine = new DevMachine();
        devMachine.setMachineId("" + new Random().nextInt(10000));
        devMachine.setMachineName("大挖");
        devMachine.setMachineType("挖掘机");
        devMachine.setDescription("挖掘机666");
        return devMachine;
    }
}

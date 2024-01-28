package com.example.producer;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

@RestController
@CrossOrigin(origins = "*", allowedHeaders = "*")
public class ProducerController {

    private final Logger logger = LoggerFactory.getLogger(ProducerController.class);
    private final KafkaTemplate<String, String> kafkaTemplate;

    public ProducerController(@Autowired KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/api/select")
    public void selectColor(
            @RequestHeader("user-agent") String userAgentName,
            @RequestParam(value = "color") String colorName,
            @RequestParam(value = "user") String userName
    ) {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
        Date now = new Date();
        Gson gson = new Gson();
        UserEventVO userEventVO = new UserEventVO(
                sdfDate.format(now),
                userAgentName,
                colorName,
                userName
        );
        String jsonColorLog = gson.toJson(userEventVO);
        CompletableFuture<SendResult<String, String>> resultFuture
                = kafkaTemplate.send("select-color", jsonColorLog);
    }
}

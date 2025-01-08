package com.example.kafka_producer_example.servive;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String,Object>template;

    public void sendMessageToTopic(String message) {
        try
        {
            //only 1 partition
            //CompletableFuture<SendResult<String, Object>> future=template.send("kafka-test-1", message);

            //3 partition
            CompletableFuture<SendResult<String, Object>> future=template.send("kafka-topic-4", message);

            future.whenComplete((result, ex) -> {
                if(ex==null)
                {
                    System.out.println("[INFO] Message sent successfully.");
                    System.out.println("[INFO] Message: " + message);
                    System.out.println("[INFO] Offset: " + result.getRecordMetadata().offset());
                }
                else
                {
                    System.err.println("[ERROR] Failed to send message.");
                    System.err.println("[ERROR] Message: " + message);
                    System.err.println("[ERROR] Reason: " + ex.getMessage());
                }
            });
        }catch(Exception e)
        {
            System.err.println("[FATAL] Critical error while sending message.");
            System.err.println("[FATAL] Message: " + message);
            System.err.println("[FATAL] Check Kafka server or configuration.");
            System.err.println("[FATAL] Error Details: " + e.getMessage());
        }
    }
}

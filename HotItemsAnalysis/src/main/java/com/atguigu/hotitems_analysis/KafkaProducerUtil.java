package com.atguigu.hotitems_analysis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class KafkaProducerUtil {


    public static void main(String[] args) throws Exception {
        writeToKafka("hotitems");


    }

    // 包装一个写入到 kafka 的方法
    public static void writeToKafka(String topic) throws Exception {

        // kafka 配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 定义 Kafka producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 用缓冲方式读取文本
        BufferedReader bufferedReader = new BufferedReader(new FileReader("D:\\\\JetBrains\\\\idea_space\\\\UserBehaviorAnalysis\\\\HotItemsAnalysis\\\\src\\\\main\\\\resources\\\\UserBehavior.csv"));

        String line;
        while ( (line = bufferedReader.readLine()) != null) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, line);
            // 用 producer 发送数据
            kafkaProducer.send(producerRecord);
        }
    }
}

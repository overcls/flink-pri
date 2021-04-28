package com.youzhu.pre2;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Flink05_Source_Kafka {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从kafka读取数据
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"pre1:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"Youzhu");
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));

        //打印
        kafkaDS.print();

        //执行
        env.execute(" ");

    }
}

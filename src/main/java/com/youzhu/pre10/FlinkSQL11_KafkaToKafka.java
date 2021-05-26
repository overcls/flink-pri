package com.youzhu.pre10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQL11_KafkaToKafka {

    /*
    可以kafka -> kafka
    可以kafka -> mysql
    写个source  写个sink即可  官网 找最新文档  相关参数
     */

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.注册俩张表 SourceTable  最简单的参数列表  也可加入 ack机制相关参数 事务相关参数
        //所有参数必须以逗号分隔
        tableEnv.executeSql("create table source_sensor (id string,ts bigint,vc int) with ("
                + "'connector' = 'kafka',"
                + "'connector.topic' = 'topic_source',"
                + "'connector.properties.bootstrap.servers' = 'pre1:9092,pre1:9093,pre1:9094',"
                + "'connector.properties.group.id' = 'youzhu',"
                + "'scan.startup-mode' = 'latest',"
                + "'format' = 'csv'"
                + ")");
        //3.注册SinkTable
        tableEnv.executeSql("create table sink_sensor (id string,ts bigint,vc int) with ("
                + "'connector' = 'kafka',"
                + "'connector.topic' = 'topic_sink',"
                + "'connector.properties.bootstrap.servers' = 'pre1:9092,pre1:9093,pre1:9094',"
                + "'format' = 'json'"
                + ")");

        //执行查询插入数据
        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id = 'ws_001'");


    }

}

package com.youzhu.pre11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQL04_EventTime_DDL {


    public static void main(String[] args) {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用DDL的方式指定事件时间字段
        tableEnv.executeSql("create table source_sensor (id string," +
                "ts bigint," +
                "vc int," +
                "rt as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "WATERMARK FOR rt as rt - INTERVAL '5' SECOND" +
                ") with ("
                + "'connector' = 'kafka',"
                + "'connector.topic' = 'topic_source',"
                + "'connector.properties.bootstrap.servers' = 'pre1:9092,pre1:9093,pre1:9094',"
                + "'connector.properties.group.id' = 'youzhu',"
                + "'scan.startup-mode' = 'latest',"
                + "'format' = 'csv'"
                + ")");

        //3.将DDL生成的表创建动态表
        Table table = tableEnv.from("source_sensor");


        //4.打印表信息
        table.printSchema();

    }

}

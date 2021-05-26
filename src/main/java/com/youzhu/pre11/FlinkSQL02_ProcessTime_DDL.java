package com.youzhu.pre11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQL02_ProcessTime_DDL {

    public static void main(String[] args) {

        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用DDL的方式指定处理时间字段
        tableEnv.executeSql("create table source_sensor (id string,ts bigint,vc int,pt as PROCTIME()) with ("
                + "'connector' = 'kafka',"
                + "'connector.topic' = 'topic_source',"
                + "'connector.properties.bootstrap.servers' = 'pre1:9092,pre1:9093,pre1:9094',"
                + "'connector.properties.group.id' = 'youzhu',"
                + "'scan.startup-mode' = 'latest',"
                + "'format' = 'csv'"
                + ")");

        //3.将DDL创建的表生成动态表
        Table table = tableEnv.from("source_sensor");

        //打印元数据信息
        table.printSchema();


    }
}

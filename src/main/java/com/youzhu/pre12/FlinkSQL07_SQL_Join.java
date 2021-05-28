package com.youzhu.pre12;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class FlinkSQL07_SQL_Join {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //获取保存状态时间  处理时间  默认值为0  flinkSQL中的状态永久保存
        System.out.println(tableEnv.getConfig().getIdleStateRetention());

        // 执行flinkSQL状态保留10s
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //2.读取端口数据创建流
        SingleOutputStreamOperator<TableA> TableADS = env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new TableA(split[0], split[1]);
                });
        SingleOutputStreamOperator<TableB> TableBDS = env.socketTextStream("localhost", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new TableB(split[0], Integer.parseInt(split[1]));
                });

        //3.将流转换为动态表
        tableEnv.createTemporaryView("tableA",TableADS);
        tableEnv.createTemporaryView("tableB",TableBDS);

        //4.双流join
        // tableEnv.sqlQuery("select * from tableA a join tableB b on a.id = b.id").execute().print();
        tableEnv.sqlQuery("select * from tableA a left join tableB b on a.id = b.id").execute().print();

        //执行任务
        env.execute();

    }
}

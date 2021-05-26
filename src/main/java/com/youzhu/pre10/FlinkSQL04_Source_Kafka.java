package com.youzhu.pre10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL04_Source_Kafka {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("test")
                .startFromEarliest()
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "pre1:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG, "youzhu"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
                //支持csv 和json格式
                //.withFormat(new Json())
                .withFormat(new Csv())
                .createTemporaryTable("sensor");

        //使用连接器创建表
        Table sensorTable = tableEnv.from("sensor");

        //查询数据
        Table selectResult = sensorTable.groupBy($("id"))
                .select($("id"), $("id").count());

        //将表转换为流进行输出
        tableEnv.toRetractStream(sensorTable, Row.class).print();

        //执行任务
        env.execute();

    }
}

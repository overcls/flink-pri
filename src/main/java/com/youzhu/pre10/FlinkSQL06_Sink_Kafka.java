package com.youzhu.pre10;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL06_Sink_Kafka {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("pre1", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //流转换为动态表
        Table sensorTable = tableEnv.fromDataStream(waterSensorDS);

        //使用tableAPI过滤出ws_001 的数据
        Table selectTable = sensorTable.where($("id").isEqual("ws_001"))
                .select($("id"),
                        $("ts"), $("vc"));


        //将selectTable写入Kafka
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("test")
                .startFromEarliest()
                .sinkPartitionerRoundRobin()  //轮询的方式往kafka写
                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pre1:9092"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
                //支持csv 和json格式  使用csv小bug  会产生空行
                //.withFormat(new Json())
                .withFormat(new Csv())
                .createTemporaryTable("sensor");
        selectTable.executeInsert("sensor");  //sink

        env.execute();


    }
}

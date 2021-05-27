package com.youzhu.pre11;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class FlinkSQL12_TableAPI_GroupWindow_SessionWindow_EventTime {


    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取文本数据转化为JavaBean,并提取时间戳生成Watermark
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        });

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                }).assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        //3.将流转换为表并指定事件时间字段
        Table table = tableEnv.fromDataStream(waterSensorDS, $("id"),
                $("ts"),
                $("vc"),
                $("rt").rowtime());

        //4.基于事件时间的会话窗口   withgap不是数据之间的差值 也不是watermark之间的差值  指的是数据和watermark之间的差值 >=5
        Table result = table.window(Session.withGap(lit(5).seconds()).on($("rt")).as("sw"))
                .groupBy($("id"), $("tw"))
                .select($("id"), $("tw"))
                .select($("id"), $("id").count());

        //转化为流进行打印
        tableEnv.toAppendStream(result, Row.class).print();

        //执行任务
        env.execute();


    }

}

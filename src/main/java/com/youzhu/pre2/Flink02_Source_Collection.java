package com.youzhu.pre2;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink02_Source_Collection {
    // Source transform Sink
    /*
    从集合当中读取数据  并行度设置为1 读取数据时顺序是固定的
     */
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //准备集合
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_002", 1577844015L, 43),
                new WaterSensor("ws_003", 1577844020L, 42));

        //从集合读取数据
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromCollection(waterSensors);

        //打印
        waterSensorDataStreamSource.print();

        //执行
        env.execute();
    }
}

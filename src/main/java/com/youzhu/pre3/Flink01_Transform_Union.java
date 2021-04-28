package com.youzhu.pre3;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_Transform_Union {
    /*
    多数据源统一的操作
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> stringDataStreamSource1 = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> stringDataStreamSource2 = env.socketTextStream("localhost", 9999);

        DataStream<String> dataStream = stringDataStreamSource1.union(stringDataStreamSource2);

        dataStream.print();

        env.execute(" ");

    }
}

package com.youzhu.pre2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_Source_Socket {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从端口获取数据
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 9999);

        //打印
        stringDataStreamSource.print();

        //执行
        env.execute(" ");
    }
}

package com.youzhu.pre2;

/*
connect 允许连接类型不一致的流  一次只能俩个流 connect流 只能调用map和flatmap 全外连接的实现
union 连接类型一致的流 union可以多流类型一致放在一起
join算子只能进行内连接
 */

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink11_Transform_Connect {
/*
双流join用到 connect
process 生命周期
        状态编程
        侧输出流
        定时器
 */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> stringDS = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> stringDataStreamSource2 = env.socketTextStream("localhost", 9999);

        //将stringDataStreamSource2转换为int类型
        SingleOutputStreamOperator<Integer> intDS = stringDataStreamSource2.map(String::length);

        ConnectedStreams<String, Integer> connect = stringDS.connect(intDS);

        SingleOutputStreamOperator<Object> resout = connect.map(new CoMapFunction<String, Integer, Object>() {
            //处理第一个流
            @Override
            public Object map1(String value) throws Exception {
                return value;
            }

            //处理第一个流
            @Override
            public Object map2(Integer value) throws Exception {
                return value;
            }
        });

        resout.print();

        env.execute();


    }
}

package com.youzhu.pre1;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_WordCount_Unbounded {

    public static void main(String[] args) throws Exception {

        //创建获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度  默认是key的hash值%核数
        env.setParallelism(1);

        //读取端口数据   nc.exe -l -p 9999
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);

        //将每行数据压平并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = socketTextStream.flatMap(new FLink02_WordCount_Bounded.LineToTupleFlatMapFunc());

        //分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //聚合   sum是子类自己的方法  这里的返回值类型不能转换为datastream  故scala
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //打印结果
        result.print();

        //启动程序
        env.execute(" ");


    }

}

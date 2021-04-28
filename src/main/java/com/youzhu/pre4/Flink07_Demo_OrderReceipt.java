package com.youzhu.pre4;

import com.youzhu.bean.OrderEvent;
import com.youzhu.bean.TxEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Flink07_Demo_OrderReceipt {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取2个文本数据创建流
        DataStreamSource<String> orderStreamDS = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> receiptStreamDS = env.readTextFile("input/ReceiptLog.csv");

        //转换为JavaBean
        SingleOutputStreamOperator<OrderEvent> orderEventSingleOutputStreamOperator = orderStreamDS.flatMap(new FlatMapFunction<String, OrderEvent>() {
            @Override
            public void flatMap(String value, Collector<OrderEvent> out) throws Exception {
                String[] split = value.split(",");

                OrderEvent orderEvent = new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));

                if ("pay".equals(orderEvent.getEventType())) {
                    out.collect(orderEvent);
                }
            }
        });

        SingleOutputStreamOperator<TxEvent> txEventSingleOutputStreamOperator = receiptStreamDS.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {

                String[] split = value.split(",");
                return new TxEvent(split[0], split[1], Long.parseLong(split[2]));
            }
        });

        //按照TxID进行分组  原因是 为了让两条流进入一个并行度中
        KeyedStream<OrderEvent, String> orderEventStringKeyedStream = orderEventSingleOutputStreamOperator.keyBy(data -> data.getTxId());

        KeyedStream<TxEvent, String> txEventStringKeyedStream = txEventSingleOutputStreamOperator.keyBy(data -> data.getTxId());

        //连接俩个流
        ConnectedStreams<OrderEvent, TxEvent> connect = orderEventStringKeyedStream.connect(txEventStringKeyedStream);

        //处理流 map数据进来要求返回值  与业务场景不适合
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = connect.process(new MyCoKeyedProcess());

        //打印结果
        result.print();

        //执行结果
        env.execute();

    }
    public static class MyCoKeyedProcess extends KeyedCoProcessFunction<String,OrderEvent,TxEvent, Tuple2<OrderEvent,TxEvent>>{

        private HashMap<String,OrderEvent> orderEventHashMap = new HashMap<>();
        private HashMap<String,TxEvent> txEventHashMap = new HashMap<>();

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

            if (txEventHashMap.containsKey(value.getTxId())){
                TxEvent txEvent = txEventHashMap.get(value.getTxId());
                out.collect(new Tuple2<>(value,txEvent));
            }else {
                orderEventHashMap.put(value.getTxId(),value);
            }
        }

        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

            if (orderEventHashMap.containsKey(value.getTxId())){
                OrderEvent orderEvent = orderEventHashMap.get(value.getTxId());
                out.collect(new Tuple2<>(orderEvent,value));
            }else {
              txEventHashMap.put(value.getTxId(),value);
            }
        }
    }

}

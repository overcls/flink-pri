package com.youzhu.pre9;

import com.youzhu.bean.OrderEvent;
import com.youzhu.bean.TxEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink01_Practice_OrderReceiptWithState {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取2个文本数据创建流
        DataStreamSource<String> orderStreamDS = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> receiptStreamDS = env.readTextFile("input/ReceiptLog.csv");

        //转换为JavaBean
        //提取数据中的时间戳生成Watermark
        WatermarkStrategy<OrderEvent> orderEventWatermarkStrategy = WatermarkStrategy.<OrderEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
            @Override
            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                return element.getEventTime() * 1000L;
            }
        });
        WatermarkStrategy<TxEvent> txEventWatermarkStrategy = WatermarkStrategy.<TxEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<TxEvent>() {
            @Override
            public long extractTimestamp(TxEvent element, long recordTimestamp) {
                return element.getEventTime() * 1000L;
            }
        });

        SingleOutputStreamOperator<OrderEvent> orderEventSingleOutputStreamOperator = orderStreamDS.flatMap(new FlatMapFunction<String, OrderEvent>() {
            @Override
            public void flatMap(String value, Collector<OrderEvent> out) throws Exception {
                String[] split = value.split(",");

                OrderEvent orderEvent = new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));

                if ("pay".equals(orderEvent.getEventType())) {
                    out.collect(orderEvent);
                }
            }
        }).assignTimestampsAndWatermarks(orderEventWatermarkStrategy);

        SingleOutputStreamOperator<TxEvent> txEventSingleOutputStreamOperator = receiptStreamDS.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {

                String[] split = value.split(",");
                return new TxEvent(split[0], split[1], Long.parseLong(split[2]));
            }
        }).assignTimestampsAndWatermarks(txEventWatermarkStrategy);

        //连接支付流和到账流
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = orderEventSingleOutputStreamOperator.connect(txEventSingleOutputStreamOperator)
                .keyBy("txId", "txId")
                .process(new OrderReceiptKeyedProcessFunc());

        //打印数据
        result.print();
        result.getSideOutput(new OutputTag<String>("Payed No Receipt") {
        }).print("No Receipt");
        result.getSideOutput(new OutputTag<String>("Receipt No Payed") {
        }).print("No Payed");
        //执行
        env.execute();

    }

    public static class OrderReceiptKeyedProcessFunc extends KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>> {

        //声明状态
        private ValueState<OrderEvent> payEventState;
        private ValueState<TxEvent> txEventState;
        //要删除状态时,一定要重新定义一个状态保存时间戳
        private ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            payEventState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("pay-state", OrderEvent.class));
            txEventState = getRuntimeContext().getState(new ValueStateDescriptor<TxEvent>("tx-state", TxEvent.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerState", Long.class));
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

            //取出到账状态数据
            TxEvent txEvent = txEventState.value();

            //判断到账数据是否已经到达
            if (txEvent == null) {//到账数据还没有到达

                //将自身存入状态
                payEventState.update(value);

                //注册定时器
                long ts = (value.getEventTime() + 10) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                timerState.update(ts);

            } else {//说明到账数据已经到达

                //结合写入主流
                out.collect(new Tuple2<>(value, txEvent));

                //删除定时器
                ctx.timerService().deleteEventTimeTimer(timerState.value());

                //清空状态
                txEventState.clear();
                timerState.clear();

            }

        }

        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

            //取出支付数据
            OrderEvent orderEvent = payEventState.value();

            //判断支付数据是否已经到达
            if (orderEvent == null) {//支付数据没有到达

                //将自身保存至状态
                txEventState.update(value);

                //注册定时器
                long ts = (value.getEventTime() + 5) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                timerState.update(ts);

            } else {//支付数据已经到达

                //结合写入主流
                out.collect(new Tuple2<>(orderEvent, value));

                //删除定时器
                ctx.timerService().deleteEventTimeTimer(timerState.value());

                //清空状态
                payEventState.clear();
                timerState.clear();

            }

        }

        //定时器方法
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

            //取出支付状态数据
            OrderEvent orderEvent = payEventState.value();
            TxEvent txEvent = txEventState.value();

            //判断orderEvent是否为null
            if (orderEvent != null) {

                ctx.output(new OutputTag<String>("Payed No Receipt") {
                           },
                        orderEvent.getTxId() + "只有支付没有到账");
            } else {


                ctx.output(new OutputTag<String>("Receipt No Payed") {
                           },
                        txEvent.getTxId() + "只有到账没有支付");

            }

        }
    }
}

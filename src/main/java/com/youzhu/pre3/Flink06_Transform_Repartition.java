package com.youzhu.pre3;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_Transform_Repartition {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//pulsar 消息队列  360转换 kafka->pulsar


        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<String> map = socketTextStream.map(x -> x).setParallelism(2);
        map.print("Map").setParallelism(2);
        map.rebalance().print("Rebalance");
        map.rescale().print("Rescale");
        //使用不同的重分区策略分区后打印 加上自定义共有八大类
       // socketTextStream.keyBy(x->x).print("KeyBy");  //按照hash 相同的Key进入相同的并行度
       // socketTextStream.shuffle().print("Shuffle");   //随机
       // socketTextStream.rebalance().print("Rebalance");  //全局轮询
       // socketTextStream.rescale().print("Rescale");   //先对自身并行度   局部轮询 再对上游下游数据每个并行度内去单独轮询 不能整除时 遵循 1好分区 发到 123 分区  2号分区 发到45分区
       // socketTextStream.global().print("Global");  //全局为1  所有数据发送到同一个并行度   全局窗口
        //socketTextStream.forward().print("Forward"); //报错 forward是一个并行度 print是8个并行度 是要求并行度上下游相同 是1对1的
       // socketTextStream.broadcast().print("Broadcast");  //所有分区都有   所有数据发送到所有并行度

        env.execute();
    }

}

package com.youzhu.pre12;

import com.youzhu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL01_ItemCountTopNWithSQL {

    public static void main(String[] args) {

        //获取执行环境  sql做不到定时器做的事情  没有允许窗口迟到  和  侧输出流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取文本数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/UserBehavior.csv");

        //转换为JavaBean根据行为过滤数据并提取时间戳生成Watermark
        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                return element.getTimestamp() * 1000L;
            }
        });
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = stringDataStreamSource.map(data -> {
            String[] split = data.split(",");
            return new UserBehavior(Long.parseLong(split[0]),
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2]),
                    split[3],
                    Long.parseLong(split[4]));
        }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        //将userBehaviorDS流转换为动态表  并指定事件时间字段
        Table table = tableEnv.fromDataStream(userBehaviorDS,
                $("userId"),
                $("itemId"),
                $("categoryId"),
                $("behavior"),
                $("timestamp"),
                $("rt").rowtime());

        //使用flinkSQL实现 滑动窗口内部计算每个商品被点击的总数
        Table windowItemCountTable = tableEnv.sqlQuery("select itemId," +
                " count(itemId) as ct," +
                " hop_end(rt, INTERVAL '5' MINUTE , INTERVAL '1' HOUR) as windowEnd " +
                " from " + table +
                " group by itemId , hop(rt,interval '5' minute , interval '1' hour)");

        //按照窗口关闭时间分组求TopN,排序  rank暂时不支持
        Table rankTable = tableEnv.sqlQuery("select " +
                " itemId ," +
                " ct," +
                " windowEnd," +
                " row_number() over(partition by windowEnd order by ct desc) as rk" +
                " from " + windowItemCountTable);
        //取TopN
        Table result = tableEnv.sqlQuery("select * from " + rankTable + " where rk <= 5");

        //打印结果
        result.execute().print();
    }
}

package com.youzhu.pre3;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Flink10_Sink_MySource {
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> streamOperator = env.socketTextStream("localhost", 9999).map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");

                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //将数据写入Mysql
        streamOperator.addSink(new MySink());

        env.execute();
    }
    public static class  MySink extends RichSinkFunction<WaterSensor> {

        //声明连接
        private Connection connection;
        //预编译
        private PreparedStatement preparedStatement;

        //生命周期方法 用于创建方法
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("","","");

            preparedStatement = connection.prepareStatement("?,?");

        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {

            //给占位符赋值
            preparedStatement.setString(1,value.getId());
            preparedStatement.setLong(2,value.getTs());
            preparedStatement.setInt(3,value.getVc());
            preparedStatement.setLong(4,value.getTs());
            preparedStatement.setInt(5,value.getVc());

            //执行操作
            preparedStatement.execute();

        }

        //生命周期方法  用于关闭连接
        @Override
        public void close() throws Exception {
            //如果是查询也需要关闭查询 resultset.close
            preparedStatement.close();
            connection.close();
        }
    }
}

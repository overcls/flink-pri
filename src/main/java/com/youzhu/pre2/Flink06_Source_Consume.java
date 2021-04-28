package com.youzhu.pre2;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Flink06_Source_Consume {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从自定义的端口中读数据
        DataStreamSource<WaterSensor> pre1 = env.addSource(new SocketSource("pre1", 9999));

        pre1.print();

        env.execute(" ");

    }

    /*
    自定义从端口中读取数据
     */
    public static class SocketSource implements SourceFunction<WaterSensor>{

        /*
        定义属性信息,主机&端口号
         */
        private String host;
        private Integer port;
        private Boolean Running = true;
        Socket socket = null;
        BufferedReader bufferedReader = null;


        public SocketSource() {
        }

        public SocketSource(String host, Integer port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            //创建输入流
            socket = new Socket(host, port);
            bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            //读取数据
            String line = bufferedReader.readLine();
            while(Running && line!=null){

                //接收数据并发送
                String[] split = line.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                ctx.collect(waterSensor);
                line = bufferedReader.readLine();

            }
        }

        @Override
        public void cancel() {
            Running = false;
            //关闭流
            try {
                bufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}

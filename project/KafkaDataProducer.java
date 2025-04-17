package com.edu.neusoft.project;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static java.util.concurrent.Executors.newFixedThreadPool;

/**
 * 读取data_format.csv的数据，向Kafka中灌入数据（多线程），模拟实时数据的产生
 */
public class KafkaDataProducer {

    public static void main(String[] args) throws IOException {

        int taskNum = 5;
        long interval = 100;

        //1.读取user_log_new.csv文件数据，完成反序列化，加载到内存中
        File sourceFile = new File(KafkaDataProducer.class.getClassLoader().getResource("user_log_new.csv").getFile());
        List<String> lines = FileUtils.readLines(sourceFile, Charset.forName("UTF-8"));
        List<UserBehaviorPojo> ubs = lines.stream().map(line -> new UserBehaviorPojo(line)).collect(Collectors.toList());

        //2.启动多线程，生产数据
        ExecutorService threadPool = newFixedThreadPool(taskNum);
        //创建多个任务
        List<Runnable> tasks = new ArrayList<>();
        for(int i = 1; i <= taskNum; i++){
            tasks.add(new ProducerTask(interval, ubs));
        }
        //线程池开始执行这些任务
        for(Runnable task : tasks){
            threadPool.execute(task);
        }

        threadPool.shutdown();
    }

}

class ProducerTask implements Runnable {

    /**
     * 间隔
     */
    long interval;

    /**
     * 总记录列表
     */
    List<UserBehaviorPojo> ubs;

    /**
     * topic
     */
    String topic;

    /**
     * Kafka Producer
     */
    KafkaProducer<String,String> producer;

    public ProducerTask(long interval, List<UserBehaviorPojo> ubs) {
        super();
        //1.初始化变量
        this.interval = interval;
        this.ubs = ubs;

        //2.初始化Kafka Producer
        String brokerList = "ubuntu:9092";
        this.topic = "realtime-data";
        Properties properties = new Properties();
        properties.put("bootstrap.servers",brokerList);
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String,String>(properties);
    }


    @Override
    public void run() {
        //发送消息
        try{
            while(true){
                //取[1-10000]之间的随机数
                Integer index = new Random().nextInt(9999)+1;
                UserBehaviorPojo ub = ubs.get(index);
                String json = JSON.toJSONString(ub);
                ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,json);
                System.out.println(Thread.currentThread().getName() + " send message:" + json);
                producer.send(record);
                //避免产生数据太快，休眠一小会
                Thread.sleep(interval);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}

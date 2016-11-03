package com.simple.kafka.consumer;

/**
 * Created by frank on 2016/11/3.
 *
 * @apiNote 消费数据接口
 */
public interface ConsumerMsgPool {

    /**
     * 接收数据
     *
     * @param msg 数据
     */
    void receiveMsg(String msg);
}

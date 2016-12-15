package com.simple.kafka.consumer;

/**
 * Created by zg on 2016/11/3.
 *
 * @apiNote 消费数据接口
 */
public interface ConsumerMsgInterface {

    /**
     * 接收数据
     *
     * @param msg 数据
     */
    void receiveMsg(String msg);
}

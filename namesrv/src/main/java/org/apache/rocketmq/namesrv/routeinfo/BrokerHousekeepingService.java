/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * nameserver实时感知着broker的生命情况,一有变动，就赶紧更新自己的那些map里面的缓存
 * 这里的变动主要是靠着channelActive   channelInactive ... 方法里面,对一个eventQueue进行填充,如果这个eventQueue里面有值,一个线程就会立马感知到,取走，从而就会调用这里面的响应的方法
 * 
 * @see NettyRemotingAbstract.NettyEventExecutor.run()
 * 上面的那个run方法是一个deamon线程，实时监控着eventQueue的情况,可以看到，queue也可以用来做多线程通信(交互)，并且还是非常棒的一种方法
 * 
 * 
 * 这个是nameserver的houseKeeping,其他的模块有自己的houseKeeping了,但是原理都是差不多的,也就是通过队列来进行通信
 * 
 * add by Hongbo Cao BrokerHousekeepingService
 *
 */
public class BrokerHousekeepingService implements ChannelEventListener {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private final NamesrvController namesrvController;

    public BrokerHousekeepingService(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {
    }

    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        this.namesrvController.getRouteInfoManager().onChannelDestroy(remoteAddr, channel);
    }

    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        this.namesrvController.getRouteInfoManager().onChannelDestroy(remoteAddr, channel);
    }

    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        this.namesrvController.getRouteInfoManager().onChannelDestroy(remoteAddr, channel);
    }
}

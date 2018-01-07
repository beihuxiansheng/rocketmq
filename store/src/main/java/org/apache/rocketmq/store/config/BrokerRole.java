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
package org.apache.rocketmq.store.config;
//config 存储的配置信息
//add by Hongbo Cao BrokerRole
public enum BrokerRole {
	//异步复制主节点
    ASYNC_MASTER,
    //同步双写主节点
    SYNC_MASTER,
    //备节点
    SLAVE;
	//其实，这些工具的写法也基本一致，都是先做一些检查，最后运行 Java 程序，JVM 系统上的应用应该差不多都这样
}

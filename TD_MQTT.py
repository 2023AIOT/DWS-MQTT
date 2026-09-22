import socket
from typing import List, Dict, Any, Set, Optional, Tuple
import threading
import time
import random
import paho.mqtt.client as mqtt
import re
import json
import logging
import os
import subprocess
import asyncio

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('TD-MQTT')

# 定义全局变量，用于存储代理节点和订阅信息
BROKER_LIST = []    # 存储活跃代理的信息
SUBSCRIPTION_TABLE = {}  # 存储主题和订阅该主题的代理节点
ROUTING_TABLE = {}  # 存储路由表，记录到达其他代理的最佳路径
CLIENTS = {}        # 存储连接的客户端信息
MQTT_PORT = 1883    # 定义默认的 MQTT 端口
BROKER_ID = ""      # 当前代理的唯一标识符
NEIGHBORS = set()   # 相邻代理节点集合
TOPOLOGY = {}       # 网络拓扑图
TD_MQTT_PROCESSES = []  # 用于跟踪启动的进程

# 锁，用于保护共享数据
subscription_lock = threading.Lock()
routing_lock = threading.Lock()
topology_lock = threading.Lock()

# 代理发现
def broker_discovery(address_range: List[str], mqtt_port: int, timeout: float = 2.0) -> List[Dict[str, Any]]:
    """发现网络中活跃的MQTT代理节点"""
    active_brokers = []
    for addr in address_range:
        logger.info(f"正在扫描 {addr}:{mqtt_port}")
        broker_info = {"addr": addr, "id": f"broker-{addr.replace('.', '-')}"}
        try:
            with socket.create_connection((addr, mqtt_port), timeout) as conn:
                logger.info(f"{addr}:{mqtt_port} 活跃")
                broker_info["status"] = "active"
                broker_info["load"] = 0  # 初始负载为0
                active_brokers.append(broker_info)
                # 添加到邻居列表
                with topology_lock:
                    NEIGHBORS.add(addr)
        except (socket.timeout, socket.error):
            logger.info(f"{addr}:{mqtt_port} 不可用")
            broker_info["status"] = "inactive"
    return active_brokers

# 分布式订阅管理
def add_subscription(broker: str, topic: str):
    """添加主题订阅到分布式订阅表
    
    Args:
        broker (str): 代理节点的地址
        topic (str): 订阅的主题
    """
    with subscription_lock:
        if topic not in SUBSCRIPTION_TABLE:
            SUBSCRIPTION_TABLE[topic] = set()  # 初始化订阅列表
        SUBSCRIPTION_TABLE[topic].add(broker)  # 添加订阅代理
        logger.info(f"添加订阅: 代理={broker}, 主题={topic}")
    
    # 通知其他代理节点有新的订阅
    broadcast_subscription_update(broker, topic, "add")

def remove_subscription(broker: str, topic: str):
    """从分布式订阅表中移除订阅
    
    Args:
        broker (str): 代理节点的地址
        topic (str): 订阅的主题
    """
    with subscription_lock:
        if topic in SUBSCRIPTION_TABLE:
            SUBSCRIPTION_TABLE[topic].discard(broker)
            logger.info(f"移除订阅: 代理={broker}, 主题={topic}")
            if not SUBSCRIPTION_TABLE[topic]:
                del SUBSCRIPTION_TABLE[topic]  # 如果没有订阅者，则删除主题
    
    # 通知其他代理节点订阅已移除
    broadcast_subscription_update(broker, topic, "remove")

def broadcast_subscription_update(broker: str, topic: str, action: str):
    """广播订阅更新信息到相邻代理节点
    
    Args:
        broker (str): 代理节点的地址
        topic (str): 订阅的主题
        action (str): 操作类型 ("add" 或 "remove")
    """
    message = {
        "type": "subscription_update",
        "broker": broker,
        "topic": topic,
        "action": action
    }
    
    # 向所有相邻代理广播
    for neighbor in NEIGHBORS:
        if neighbor != BROKER_ID:  # 不发送给自己
            try:
                # 创建兼容新版本的MQTT客户端
                try:
                    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
                except (AttributeError, TypeError):
                    client = mqtt.Client()
                client.connect(neighbor, MQTT_PORT)
                client.publish("$SYS/td-mqtt/control", json.dumps(message))
                client.disconnect()
            except Exception as e:
                logger.error(f"广播订阅更新失败: {e}")

# 主题匹配
def topic_matches(subscription_topic: str, published_topic: str) -> bool:
    """检查发布的主题是否匹配订阅主题（支持通配符）
    
    Args:
        subscription_topic: 订阅主题，可能包含通配符
        published_topic: 发布的主题，不含通配符
        
    Returns:
        bool: 如果主题匹配返回True，否则返回False
    """
    # 转换MQTT通配符到正则表达式
    if subscription_topic == "#":
        return True
    
    regex = subscription_topic
    regex = regex.replace("+", "[^/]+")
    regex = regex.replace("#", ".*")
    
    # 确保完全匹配
    regex = f"^{regex}$"
    return bool(re.match(regex, published_topic))

# 消息路由
def route_message(topic: str, message: str, source_broker: Optional[str] = None):
    """根据分布式订阅表将消息路由到订阅该主题的代理
    
    Args:
        topic (str): 消息的主题
        message (str): 消息内容
        source_broker (str, optional): 消息来源的代理，防止循环路由
    """
    # 查找所有匹配的主题
    subscribers = set()
    with subscription_lock:
        for sub_topic, brokers in SUBSCRIPTION_TABLE.items():
            if topic_matches(sub_topic, topic):
                subscribers.update(brokers)
    
    if not subscribers:
        logger.info(f"无代理订阅主题 {topic}")
        return
    
    # 获取所有订阅该主题的代理
    for broker in subscribers:
        # 避免消息循环路由
        if broker == source_broker:
            continue
            
        logger.info(f"将消息发送到代理 {broker}, 主题: {topic}")
        try:
            # 通过MQTT转发消息，创建兼容新版本的客户端
            try:
                client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
            except (AttributeError, TypeError):
                client = mqtt.Client()
            client.connect(broker, MQTT_PORT)
            client.publish(topic, message)
            client.disconnect()
        except Exception as e:
            logger.error(f"发送消息到代理 {broker} 失败: {e}")

# 拓扑管理
def update_topology(broker_id: str, neighbors: List[str]):
    """更新网络拓扑信息
    
    Args:
        broker_id (str): 报告拓扑的代理ID
        neighbors (List[str]): 该代理的邻居列表
    """
    with topology_lock:
        TOPOLOGY[broker_id] = neighbors
        
    # 更新路由表
    update_routing_table()

def update_routing_table():
    """基于拓扑信息更新路由表（使用简化的Dijkstra算法）"""
    with routing_lock:
        # 重置路由表
        global ROUTING_TABLE
        ROUTING_TABLE = {BROKER_ID: {"next_hop": None, "distance": 0}}
        
        # 使用Dijkstra算法计算最短路径
        unvisited = set(TOPOLOGY.keys())
        current = BROKER_ID
        
        while unvisited:
            if current in unvisited:
                unvisited.remove(current)
                
                # 获取当前节点的距离
                current_distance = ROUTING_TABLE[current]["distance"]
                
                # 检查邻居
                if current in TOPOLOGY:
                    for neighbor in TOPOLOGY[current]:
                        distance = current_distance + 1  # 简单地假设每条边的权重为1
                        
                        if neighbor not in ROUTING_TABLE or distance < ROUTING_TABLE[neighbor]["distance"]:
                            ROUTING_TABLE[neighbor] = {
                                "next_hop": ROUTING_TABLE[current]["next_hop"] if current != BROKER_ID else neighbor,
                                "distance": distance
                            }
            
            # 选择下一个最近的未访问节点
            min_distance = float('inf')
            next_node = None
            
            for node in unvisited:
                if node in ROUTING_TABLE and ROUTING_TABLE[node]["distance"] < min_distance:
                    min_distance = ROUTING_TABLE[node]["distance"]
                    next_node = node
            
            if next_node is None:
                break  # 没有更多可达节点
                
            current = next_node
        
        logger.info(f"更新路由表: {ROUTING_TABLE}")

# 负载均衡
def select_broker_for_client(client_id: str) -> str:
    """为客户端选择最合适的代理节点
    
    实现更智能的负载均衡算法，考虑代理节点的负载和网络延迟
    
    Args:
        client_id (str): 客户端ID
        
    Returns:
        str: 选择的代理节点地址
    """
    if not BROKER_LIST:
        raise Exception("没有可用的代理节点")

    # 高级负载均衡策略：考虑负载和随机因素
    # 1. 过滤负载过高的代理
    available_brokers = [b for b in BROKER_LIST if b.get("load", 0) < 10]  # 负载阈值
    
    if not available_brokers:
        # 如果所有代理都负载过高，选择负载最低的
        selected_broker = min(BROKER_LIST, key=lambda x: x.get("load", 0))
    else:
        # 从可用代理中加权随机选择，权重与负载成反比
        weights = [10 - b.get("load", 0) for b in available_brokers]
        selected_broker = random.choices(available_brokers, weights=weights, k=1)[0]
    
    # 更新选择的代理负载
    selected_broker["load"] = selected_broker.get("load", 0) + 1
    
    logger.info(f"为客户端 {client_id} 分配代理 {selected_broker['addr']}")
    return selected_broker['addr']

def release_broker_load(broker_addr: str):
    """客户端断开连接时减少代理负载"""
    for broker in BROKER_LIST:
        if broker["addr"] == broker_addr:
            broker["load"] = max(0, broker.get("load", 0) - 1)
            break

# 客户端会话管理
def register_client(client_id: str, broker_addr: str):
    """注册客户端连接信息"""
    CLIENTS[client_id] = {
        "broker": broker_addr,
        "connected": True,
        "last_seen": time.time(),
        "subscriptions": []
    }

def unregister_client(client_id: str):
    """客户端断开连接时更新信息"""
    if client_id in CLIENTS:
        CLIENTS[client_id]["connected"] = False
        CLIENTS[client_id]["last_seen"] = time.time()
        broker_addr = CLIENTS[client_id]["broker"]
        release_broker_load(broker_addr)

def add_client_subscription(client_id: str, topic: str):
    """添加客户端的订阅信息"""
    if client_id in CLIENTS:
        if topic not in CLIENTS[client_id]["subscriptions"]:
            CLIENTS[client_id]["subscriptions"].append(topic)
            # 添加到分布式订阅表
            broker_addr = CLIENTS[client_id]["broker"]
            add_subscription(broker_addr, topic)

def remove_client_subscription(client_id: str, topic: str):
    """移除客户端的订阅信息"""
    if client_id in CLIENTS and topic in CLIENTS[client_id]["subscriptions"]:
        CLIENTS[client_id]["subscriptions"].remove(topic)
        # 检查是否还有其他客户端通过同一代理订阅此主题
        broker_addr = CLIENTS[client_id]["broker"]
        clients_on_same_broker = [
            cid for cid, info in CLIENTS.items() 
            if info["broker"] == broker_addr and topic in info["subscriptions"]
        ]
        
        if not clients_on_same_broker:
            # 如果没有其他客户端，从分布式订阅表中移除
            remove_subscription(broker_addr, topic)

# 控制消息处理
def handle_control_message(client, userdata, msg):
    """处理TD-MQTT控制消息"""
    try:
        payload = json.loads(msg.payload)
        msg_type = payload.get("type")
        
        if msg_type == "subscription_update":
            broker = payload.get("broker")
            topic = payload.get("topic")
            action = payload.get("action")
            
            if action == "add":
                with subscription_lock:
                    if topic not in SUBSCRIPTION_TABLE:
                        SUBSCRIPTION_TABLE[topic] = set()
                    SUBSCRIPTION_TABLE[topic].add(broker)
            elif action == "remove":
                with subscription_lock:
                    if topic in SUBSCRIPTION_TABLE:
                        SUBSCRIPTION_TABLE[topic].discard(broker)
                        if not SUBSCRIPTION_TABLE[topic]:
                            del SUBSCRIPTION_TABLE[topic]
        
        elif msg_type == "topology_update":
            broker_id = payload.get("broker_id")
            neighbors = payload.get("neighbors")
            update_topology(broker_id, neighbors)
            
        elif msg_type == "broker_status":
            broker_addr = payload.get("addr")
            status = payload.get("status")
            
            for broker in BROKER_LIST:
                if broker["addr"] == broker_addr:
                    broker["status"] = status
                    # 如果代理下线，移除相关订阅
                    if status == "inactive":
                        with subscription_lock:
                            for topic in list(SUBSCRIPTION_TABLE.keys()):
                                if broker_addr in SUBSCRIPTION_TABLE[topic]:
                                    SUBSCRIPTION_TABLE[topic].discard(broker_addr)
                                    if not SUBSCRIPTION_TABLE[topic]:
                                        del SUBSCRIPTION_TABLE[topic]
                    break
    
    except Exception as e:
        logger.error(f"处理控制消息出错: {e}")

# 代理节点模拟
def simulate_broker(broker_address: str):
    """模拟代理节点的运行，处理订阅和消息发布"""
    global BROKER_ID
    BROKER_ID = broker_address  # 设置当前代理ID
    
    def on_connect(client, userdata, flags, rc):
        logger.info(f"[代理 {broker_address}] 已连接, 返回码={rc}")
        # 订阅控制主题
        client.subscribe("$SYS/td-mqtt/control")
        
        # 广播代理状态
        status_msg = {
            "type": "broker_status",
            "addr": broker_address,
            "status": "active",
            "time": time.time()
        }
        client.publish("$SYS/td-mqtt/control", json.dumps(status_msg))
        
        # 广播拓扑信息
        topology_msg = {
            "type": "topology_update",
            "broker_id": broker_address,
            "neighbors": list(NEIGHBORS)
        }
        client.publish("$SYS/td-mqtt/control", json.dumps(topology_msg))

    def on_message(client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode("utf-8")
        
        # 处理控制消息
        if topic == "$SYS/td-mqtt/control":
            handle_control_message(client, userdata, msg)
            return
            
        logger.info(f"[代理 {broker_address}] 收到消息: 主题={topic}, 内容={payload}")
        
        # 路由消息到其他订阅该主题的代理
        route_message(topic, payload, broker_address)

    def on_subscribe(client, userdata, mid, granted_qos):
        logger.info(f"[代理 {broker_address}] 订阅成功, mid={mid}")

    def on_disconnect(client, userdata, rc):
        if rc != 0:
            logger.warning(f"[代理 {broker_address}] 意外断开连接, 返回码={rc}")
        else:
            logger.info(f"[代理 {broker_address}] 断开连接")

    # 创建MQTT客户端，添加callback_api_version参数以兼容Paho MQTT 2.0版本
    try:
        # 尝试使用新版API
        client = mqtt.Client(client_id=f"td-mqtt-broker-{broker_address}", callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
    except (AttributeError, TypeError):
        # 如果失败，说明是旧版本，使用旧API
        client = mqtt.Client(client_id=f"td-mqtt-broker-{broker_address}")
        
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect
    
    # 绑定代理的 MQTT 服务
    try:
        client.connect(broker_address, MQTT_PORT)
        client.loop_start()
        
        # 定期广播活跃状态
        while True:
            time.sleep(60)  # 每分钟
            status_msg = {
                "type": "broker_status",
                "addr": broker_address,
                "status": "active",
                "time": time.time()
            }
            client.publish("$SYS/td-mqtt/control", json.dumps(status_msg))
            
    except Exception as e:
        logger.error(f"[代理 {broker_address}] 连接失败: {e}")

# 定期检查代理状态
def check_broker_status():
    """定期检查代理节点状态"""
    while True:
        time.sleep(120)  # 每2分钟检查一次
        
        for broker in BROKER_LIST:
            addr = broker["addr"]
            try:
                with socket.create_connection((addr, MQTT_PORT), timeout=2.0):
                    if broker["status"] != "active":
                        logger.info(f"代理 {addr} 重新上线")
                        broker["status"] = "active"
                        # 广播状态更新
                        broadcast_broker_status(addr, "active")
            except (socket.timeout, socket.error):
                if broker["status"] == "active":
                    logger.warning(f"代理 {addr} 已下线")
                    broker["status"] = "inactive"
                    # 广播状态更新
                    broadcast_broker_status(addr, "inactive")

def broadcast_broker_status(addr: str, status: str):
    """广播代理状态更新"""
    message = {
        "type": "broker_status",
        "addr": addr,
        "status": status,
        "time": time.time()
    }
    
    # 向所有相邻代理广播
    for neighbor in NEIGHBORS:
        if neighbor != addr:  # 不发送给自己或已下线的代理
            try:
                # 创建兼容新版本的MQTT客户端
                try:
                    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
                except (AttributeError, TypeError):
                    client = mqtt.Client()
                client.connect(neighbor, MQTT_PORT)
                client.publish("$SYS/td-mqtt/control", json.dumps(message))
                client.disconnect()
            except Exception as e:
                logger.error(f"广播状态更新失败: {e}")

async def setup_td_mqtt_brokers(broker_address="localhost"):
    """
    设置并启动TD-MQTT代理
    
    Args:
        broker_address: 代理地址，默认为localhost
    """
    print(f"正在设置TD-MQTT代理: {broker_address}")
    
    # 创建临时配置文件目录
    os.makedirs("mosquitto-td-configs", exist_ok=True)
    
    # 定义要启动的代理端口
    broker_ports = [1883, 1884, 1885]
    td_processes = []
    
    try:
        # 为每个端口启动一个Mosquitto代理
        for port in broker_ports:
            config_path = f"mosquitto-td-configs/mosquitto_td_{port}.conf"
            
            # 创建配置文件
            with open(config_path, 'w') as f:
                f.write(f"""
# TD-MQTT Broker Configuration (Port {port})
port {port}
allow_anonymous true
persistence false
connection_messages true
log_type all
                """)
            
            print(f"启动TD-MQTT代理于端口 {port}...")
            
            # 启动Mosquitto进程
            process = subprocess.Popen(
                ["mosquitto", "-c", config_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            td_processes.append(process)
            print(f"Mosquitto代理已启动，PID: {process.pid}")
            
            # 创建全局变量存储进程列表，以便后续清理
            global TD_MQTT_PROCESSES
            TD_MQTT_PROCESSES = td_processes
        
        # 等待代理启动
        await asyncio.sleep(3)
        
        # 设置地址范围用于代理发现
        address_range = [f"{broker_address}" if port == 1883 else f"{broker_address}:{port}" 
                        for port in broker_ports]
        
        # 发现代理并初始化
        print("开始代理发现...")
        discovered_brokers = broker_discovery(address_range, MQTT_PORT)
        global BROKER_LIST
        BROKER_LIST = [broker for broker in discovered_brokers if broker["status"] == "active"]
        print(f"发现的代理节点: {BROKER_LIST}")
        
        # 启动代理状态检查线程
        threading.Thread(target=check_broker_status, daemon=True).start()
        
        # 启动模拟代理服务
        threads = []
        for broker in BROKER_LIST:
            thread = threading.Thread(target=simulate_broker, args=(broker["addr"],))
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        # 等待代理完全启动
        await asyncio.sleep(2)
        print("TD-MQTT代理设置完成")
        
        # 返回模拟代理程序以保持其运行
        return simulate_broker_service(threads)
        
    except Exception as e:
        print(f"设置TD-MQTT代理时出错: {e}")
        # 清理已启动的进程
        for process in td_processes:
            try:
                process.terminate()
            except:
                pass
        raise

async def simulate_broker_service(threads):
    """保持模拟代理服务运行"""
    try:
        # 这个任务会一直运行，直到被取消
        while True:
            await asyncio.sleep(10)
            # 检查线程状态
            active_threads = sum(1 for t in threads if t.is_alive())
            print(f"TD-MQTT活跃代理线程: {active_threads}/{len(threads)}")
    except asyncio.CancelledError:
        print("TD-MQTT代理服务被取消")
        raise
    finally:
        # 清理资源
        await shutdown_td_mqtt_brokers()

async def shutdown_td_mqtt_brokers():
    """关闭所有TD-MQTT代理"""
    print("正在关闭TD-MQTT代理...")
    
    # 向所有代理发送关闭信号
    try:
        for broker in BROKER_LIST:
            addr = broker["addr"]
            if ":" in addr:
                host, port = addr.split(":")
                port = int(port)
            else:
                host, port = addr, 1883
                
            # 创建客户端发送关闭信号
            try:
                client = mqtt.Client()
            except TypeError:
                try:
                    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
                except:
                    # 对于旧版本
                    client = mqtt.Client()
                    
            try:
                client.connect(host, port)
                client.publish("$SYS/td-mqtt/control", json.dumps({"type": "shutdown"}))
                client.disconnect()
                print(f"已发送关闭信号到 {addr}")
            except Exception as e:
                print(f"发送关闭信号到 {addr} 失败: {e}")
    except Exception as e:
        print(f"发送关闭信号时出错: {e}")
    
    # 终止所有Mosquitto进程
    if 'TD_MQTT_PROCESSES' in globals():
        for process in TD_MQTT_PROCESSES:
            try:
                process.terminate()
                print(f"已终止Mosquitto进程 PID: {process.pid}")
            except Exception as e:
                print(f"终止Mosquitto进程失败: {e}")
    
    print("TD-MQTT代理关闭完成")

if __name__ == "__main__":
    # 单独运行模块时执行
    try:
        asyncio.run(setup_td_mqtt_brokers())
    except KeyboardInterrupt:
        print("\nTD-MQTT服务被用户中断")
    except Exception as e:
        print(f"TD-MQTT服务出错: {e}")

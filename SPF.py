import subprocess
import asyncio
import re
from collections import namedtuple
import heapq
import time
import os
import socket

# 代理的结构
# BrokerNode = namedtuple('BrokerNode', ['id', 'priority', 'address', 'port', 'rtt', 'config_path'])
# 修改 BrokerNode 类，添加 SPF 属性
class BrokerNode:
    def __init__(self, id, priority, address, port, rtt, config_path):
        self.id = id
        self.priority = priority
        self.address = address
        self.port = port
        self.rtt = rtt
        self.config_path = config_path
        self.spf = SPF()  # 为每个代理添加 SPF 属性

# SPF 类
class SPF:
    def __init__(self):
        self.Tlb = {}  # 主题订阅列表
        self.HT = {}  # 代理的主题映射
        self.HS = {}  # 代理的泛洪组
        self.upstream_broker = None  # 上游代理
        self.upstream_topics = set()  # 上游代理已订阅的主题

    # 设置上游代理
    def set_upstream_broker(self, broker_id):
        self.upstream_broker = broker_id
        print(f"设置代理 {broker_id} 为上游代理")

    # 更新上游代理已订阅的主题
    def update_upstream_topics(self, topic):
        self.upstream_topics.add(topic)
        print(f"更新上游代理已订阅主题: {topic}")

    def subscription(self, s, broker_i, rtt_dict, stp_tree):
        ts = s["topic"]  # 获取订阅的主题
        subscriber = broker_i
        self.Tlb.setdefault(ts, set()).add(subscriber)
        self.HT.setdefault(broker_i, set()).add(ts)

        # 更新当前代理的泛洪组
        self.HS.setdefault(broker_i, set())
        
        # 情景1: 检查是否有更宽泛的主题，如果有则不添加新主题到本地洪泛组
        local_has_broader_topic = any(ts_ <= ts for ts_ in self.HS[broker_i])
        
        if not local_has_broader_topic:
            # 本地没有更广泛的主题，添加到泛洪组
            self.HS[broker_i].add(ts)
            # 移除比新主题更具体的主题
            for ts_ in list(self.HS[broker_i]):
                if ts_ > ts:
                    self.HS[broker_i].remove(ts_)
        
        # 修改点：无论本地是否有更广泛主题，都检查上游代理情况
        # 即使本地有broader topic，上游可能没有，也需要确保传播
        if self.should_forward_subscription(broker_i, ts, stp_tree):
            self.forward_subscription_to_upstream(ts)
            print(f"向上游转发订阅: {ts}，本地是否有更广泛主题: {local_has_broader_topic}")
        else:
            print(f"不需要向上游转发订阅: {ts}，本地是否有更广泛主题: {local_has_broader_topic}")

    def publication(self, p, broker_i):
        tp = p["topic"]
        targets = self.Tlb.get(tp, set())
        if targets:
            for target in targets:
                if target != broker_i:
                    self.forward_publication(p, target)

    def unsubscription(self, us, broker_i):
        tus = us["topic"]
        subscriber = broker_i

        # 从 Tlb 中移除订阅
        self.Tlb[tus].remove(subscriber)

        if subscriber == broker_i and tus in self.HT.get(broker_i, set()):
            self.HT[broker_i].remove(tus)
            if not self.HT[broker_i]:
                del self.HT[broker_i]

        if tus in self.HS.get(broker_i, set()):
            self.HS[broker_i].remove(tus)
            if not self.HS[broker_i]:
                del self.HS[broker_i]

        if not self.Tlb[tus]:
            del self.Tlb[tus]

    def add_to_flooding_group(self, broker, topic):
        print(f"\n添加主题'{topic}'到代理 {broker} 的泛洪组。")

    def forward_publication(self, publication, target):
        print(f"将发布消息 '{publication}' 转发到 {target} 。")

    def print_state(self):
        print("\n当前状态:")
        print(f"Tlb: {self.Tlb}")
        print(f"HT: {self.HT}")
        print(f"HS: {self.HS}")

    def should_subscribe(self, broker_id, topic, rtt_dict, stp_tree):
        """ 动态决定是否订阅特定主题 """
        parent_id, cost = stp_tree.get(broker_id, (None, float('inf')))

        # 选择性订阅的逻辑：如距离根代理较近、RTT较低则优先订阅
        if parent_id is None:
            return False  # 根代理不需要选择性订阅（假设根代理处理所有消息）

        # 使用RTT和STP计算选择性订阅
        rtt = rtt_dict.get(broker_id, float('inf'))
        # 基于RTT和父代理的关系动态判断
        return rtt < 100  # 如果RTT小于100ms则认为是"快速"代理，需要订阅更多的主题

    def should_forward_subscription(self, broker_id, topic, stp_tree=None):
        """判断是否需要向上游代理转发订阅"""
        # 如果没有上游代理，则不需要转发
        if self.upstream_broker is None:
            return False
            
        # 如果是根代理，则不需要转发
        if stp_tree:
            parent_id, _ = stp_tree.get(broker_id, (None, float('inf')))
            if parent_id is None:
                return False
            
        # 检查上游代理是否已有此主题或更广泛的主题
        for upstream_topic in self.upstream_topics:
            # 如果上游已有相同或更广泛的主题，不需要转发
            if upstream_topic <= topic:
                print(f"上游代理已有更广泛的主题 {upstream_topic}，不需要转发 {topic}")
                return False
                
        # 对于通配符主题，需要特别处理
        # 如果是具体主题如 test/temp/1，检查上游是否有 test/temp/+ 或 test/#
        if '+' not in topic and '#' not in topic:
            # 构造可能覆盖此主题的通配符模式
            topic_parts = topic.split('/')
            
            # 检查 test/temp/+ 类型模式
            if len(topic_parts) > 1:
                wildcard_plus = '/'.join(topic_parts[:-1]) + '/+'
                if wildcard_plus in self.upstream_topics:
                    print(f"上游代理已有通配符主题 {wildcard_plus}，不需要转发 {topic}")
                    return False
            
            # 检查 test/# 类型模式
            for i in range(len(topic_parts)):
                wildcard_hash = '/'.join(topic_parts[:i+1]) + '/#'
                if wildcard_hash in self.upstream_topics:
                    print(f"上游代理已有通配符主题 {wildcard_hash}，不需要转发 {topic}")
                    return False
        
        # 如果上游没有相关主题，则需要转发
        print(f"上游代理没有相关主题，需要转发 {topic}")
        return True

    def forward_subscription_to_upstream(self, topic):
        """向上游代理转发订阅请求"""
        if self.upstream_broker is not None:
            print(f"向上游代理 {self.upstream_broker} 转发订阅: {topic}")
            # 这里应该实现实际的转发逻辑，例如通过MQTT发送订阅请求
            # ...
            
            # 更新上游代理已订阅主题列表
            self.update_upstream_topics(topic)

async def measure_real_rtt(address, port, count=4, timeout=1.0):
    rtts = []
    for _ in range(count):
        try:
            start = time.time()
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((address, port))
            sock.close()
            end = time.time()
            rtt = (end - start) * 1000  # ms
            rtts.append(rtt)
            await asyncio.sleep(0.1)
        except Exception:
            rtts.append(timeout * 1000)
    avg_rtt = sum(rtts) / len(rtts)
    return avg_rtt

async def measure_ping_rtt(address, broker, count=4):
    avg_rtt = await measure_real_rtt(address, broker.port, count)
    print(f"真实测量RTT到代理 {broker.id}: {avg_rtt:.2f} ms")
    return broker.id, avg_rtt

# 测量所有代理的RTT
async def measure_broker_rtts(brokers, root_broker):
    await asyncio.sleep(10)  # 等待10秒，确保代理启动完成

    # 获取每个代理与根代理的RTT
    tasks = []
    for broker in brokers:
        if broker.id != root_broker.id:  # 排除根代理自身
            full_address = broker.address
            tasks.append(measure_ping_rtt(full_address, broker))  # 异步测量 RTT

    results = await asyncio.gather(*tasks)  # 等待所有 RTT 测量完成
    rtt_dict = {broker_id: rtt for broker_id, rtt in results if rtt is not None}
    return rtt_dict


# 选举根代理
async def elect_root_broker(brokers):
    # 按照优先级、id选择根代理
    root_broker = min(brokers, key=lambda b: (b.priority, b.id))
    print(f"Root broker elected: Broker {root_broker.id}")
    return root_broker

def calculate_shortest_paths(brokers, root_broker, rtt_dict):
    shortest_paths = {}
    for broker in brokers:
        if broker.id == root_broker.id:
            shortest_paths[broker.id] = (None, 0)  # 根代理的父代理为 None，距离为 0
        else:
            # 使用 RTT 计算到其他代理的距离
            parent_id, min_rtt = min(
                [(b.id, rtt_dict.get(b.id, float('inf'))) for b in brokers if b.id != broker.id],
                key=lambda x: x[1],
                default=(None, float('inf'))
            )
            shortest_paths[broker.id] = (parent_id, min_rtt)
    return shortest_paths


# 生成最短路径树STP树
def generate_stp_tree(brokers, root_broker, shortest_paths):
    mst = {broker.id: [] for broker in brokers}  # 初始化每个代理的邻接列表
    visited = set([root_broker.id])

    for broker in brokers:
        if broker.id == root_broker.id:
            continue
        # 获取到父节点的路径
        parent_broker_id = shortest_paths[broker.id][0]
        if parent_broker_id is not None:
            mst[parent_broker_id].append(broker.id)
            visited.add(broker.id)

    return mst

# 生成桥接配置
def generate_bridge_config(broker, root_broker):
    """ 根据选举的根代理和当前代理生成桥接配置 """
    if broker.id == root_broker.id:
        # 根代理不需要桥接配置，只有自身的配置
        return {
            'name': f'{broker.id}',  # 根代理的配置
            'listener': f'localhost:{broker.port}',  # 当前代理监听端口
            'remote': '',  # 根代理没有远程连接
            'topic': '#',  # 订阅所有主题
            'qos': 2,
            'is_root': True  # 标记为根代理
        }
    else:
        # 非根代理需要桥接配置
        return {
            'name': f'bridge_{broker.id}_to_{root_broker.id}',  # 桥接名称
            'listener': f'localhost:{broker.port}',  # 当前代理监听端口
            'remote': f'localhost:{root_broker.port}',  # 根代理地址
            'topic': '#',  # 订阅所有主题
            'qos': 0,
            'is_root': False  # 标记为非根代理
        }

# 写入代理配置文件
def write_config_to_file(config, broker_config_path):
    """ 将代理的桥接配置写入文件 """
    with open(broker_config_path, 'w') as config_file:
        # 添加监听器配置
        config_file.write(f"# Listener for Broker {config['name']}\n")
        config_file.write(f"listener {config['listener'].split(':')[1]}\n")
        config_file.write(f"allow_anonymous true\n")
        config_file.write(f"persistence true\n")
        config_file.write(f"persistence_location C:\\Program Files\\mosquitto\n")  # 添加持久化路径

        # QoS 2 相关配置
        config_file.write(f"max_queued_messages 1000\n")
        config_file.write(f"max_inflight_messages 20\n")
        config_file.write(f"max_packet_size 268435455\n")
        config_file.write(f"retry_interval 20\n")  # 添加重试间隔
        config_file.write(f"connection_messages true\n")  # 启用连接消息日志

        # 如果是根代理，则不写入桥接配置
        if config['remote']:  # 如果有远程连接，说明是非根代理，需要桥接配置
            config_file.write(f"\n# Bridge configuration: {config['name']}\n")
            config_file.write(f"connection {config['name']}\n")
            config_file.write(f"address {config['remote']}\n")
            config_file.write(f"topic {config['topic']} both {config['qos']}\n")
            config_file.write(f"keepalive_interval 60\n")
            config_file.write(f"bridge_insecure false\n")
            config_file.write(f"cleansession false\n")
            config_file.write(f"start_type automatic\n")  # 自动启动桥接
            config_file.write(f"bridge_protocol_version mqttv311\n")  # 指定协议版本
            config_file.write(f"try_private false\n")  # 允许共享连接

# 启动代理
def start_broker(broker_id, config_path):
    print(f"Starting Broker {broker_id} with config: {config_path}")
    try:
        process = subprocess.Popen([r"C:\\Program Files\\mosquitto\\mosquitto.exe", "-c", config_path],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return process  # 返回进程对象
    except Exception as e:
        print(f"Error starting Broker {broker_id}: {e}")
        return None

# 处理代理进程的输出
def handle_process_output(process):
    if process:
        stdout, stderr = process.communicate()
        print(f"stdout: {stdout}")
        print(f"stderr: {stderr}")
        if process.returncode != 0:
            print(f"Broker failed to start with error: {stderr}")
        else:
            print(f"Broker started successfully.")

# 监测代理变化
def detect_broker_changes(old_brokers, new_brokers):
    old_broker_ids = {broker.id for broker in old_brokers}
    new_broker_ids = {broker.id for broker in new_brokers}
    added_brokers = [broker for broker in new_brokers if broker.id not in old_broker_ids]
    removed_brokers = [broker for broker in old_brokers if broker.id not in new_broker_ids]
    return added_brokers, removed_brokers

# 重启 Mosquitto 服务
def restart_mosquitto():
    """
    尝试重启 Mosquitto 服务
    如果没有管理员权限，则跳过重启步骤
    """
    try:
        # 首先尝试使用 taskkill 结束进程（不需要管理员权限）
        subprocess.run('taskkill /F /IM mosquitto.exe',
                       shell=True,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
        print("Stopped existing Mosquitto processes")

        # 等待进程完全终止
        time.sleep(2)

    except Exception as e:
        print(f"Warning: Could not stop Mosquitto processes: {e}")
    # 不再尝试使用服务命令，而是依赖于后续的代理启动过程

# 监测代理数量变化并更新RTT和生成树
async def update_broker_topology(old_brokers, new_brokers, root_broker):
    try:
        added_brokers, removed_brokers = detect_broker_changes(old_brokers, new_brokers)

        if added_brokers or removed_brokers:
            print("Broker topology has changed. Re-calculating RTT and shortest paths.")

            # 使用新的重启方法
            restart_mosquitto()
            await asyncio.sleep(3)  # 等待进程完全终止

            # 测量新的RTT
            rtt_dict = await measure_broker_rtts(new_brokers, root_broker)
            if not rtt_dict:
                print("Using default RTT values")
                rtt_dict = {broker.id: 100 for broker in new_brokers}

            shortest_paths = calculate_shortest_paths(new_brokers, root_broker, rtt_dict)
            stp_tree = generate_stp_tree(new_brokers, root_broker, shortest_paths)

            # 按优先级排序处理代理
            sorted_brokers = sorted(new_brokers, key=lambda x: x.priority)

            # 确保配置目录存在
            config_dir = os.path.dirname(sorted_brokers[0].config_path)
            os.makedirs(config_dir, exist_ok=True)

            for broker in sorted_brokers:
                try:
                    # 生成并写入配置
                    config = generate_bridge_config(broker, root_broker)
                    write_config_to_file(config, broker.config_path)
                    print(f"Config written for Broker {broker.id}: {broker.config_path}")

                    # 初始化SPF并更新订阅状态
                    broker.spf = SPF()
                    default_subscription = {"topic": "#"}
                    broker.spf.subscription(
                        default_subscription,
                        broker.id,
                        rtt_dict,
                        stp_tree
                    )

                    # 计算最短路径后，设置上游代理
                    if broker.id != root_broker.id:  # 根代理没有上游
                        parent_id, _ = shortest_paths.get(broker.id, (None, 0))
                        if parent_id is not None:
                            broker.spf.set_upstream_broker(parent_id)
                            print(f"设置代理 {broker.id} 的上游为 {parent_id}")
                except Exception as e:
                    print(f"Error configuring broker {broker.id}: {e}")
                    continue

            # 启动代理
            processes = []
            for broker in sorted_brokers:
                try:
                    process = start_broker(broker.id, broker.config_path)
                    if process:
                        processes.append(process)
                        print(f"Started broker {broker.id}")
                    await asyncio.sleep(3)  # 增加启动间隔，确保稳定性
                except Exception as e:
                    print(f"Error starting broker {broker.id}: {e}")
                    continue

            print("Broker topology update completed successfully")
        else:
            print("No changes in broker topology.")

    except Exception as e:
        print(f"Error in updating broker topology: {str(e)}")
        import traceback
        print(traceback.format_exc())

# 配置代理和桥接
async def setup_brokers():
    brokers = [
        BrokerNode(id=1884, priority=1, address="localhost", port=1884, rtt=0,
                   config_path=r"D:\\mosquitto-configs\\mosquitto_broker1.conf"),
        BrokerNode(id=1885, priority=2, address="localhost", port=1885, rtt=0,
                   config_path=r"D:\\mosquitto-configs\\mosquitto_broker2.conf"),
        BrokerNode(id=1886, priority=3, address="localhost", port=1886, rtt=0,
                   config_path=r"D:\\mosquitto-configs\\mosquitto_broker3.conf"),
        # BrokerNode(id=1887, priority=4, address="localhost", port=1887, rtt=0,config_path=r"D:\\mosquitto-configs\\mosquitto_broker4.conf")
    ]

    # 选举根代理
    root_broker = await elect_root_broker(brokers)

    # 初始配置
    await update_broker_topology([], brokers, root_broker)  # 第一次加载所有代理并计算最短路径和树

def main():
    # 运行代理设置函数
    asyncio.run(setup_brokers())

# 主程序
if __name__ == "__main__":
    print("Starting the setup process!")
    main()
    print("Setup completed!")
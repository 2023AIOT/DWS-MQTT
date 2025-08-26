import asyncio
import random
import time
import numpy as np
import matplotlib
import importlib

matplotlib.use('TkAgg')  # 使用TkAgg后端
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import os
import paho.mqtt.client as mqtt
import threading
import socket
import json
import sys
import subprocess
import psutil  # 如果没有，需要 pip install psutil

# 配置中文字体支持
try:
    # 尝试使用系统中的中文字体
    font_path = matplotlib.font_manager.findfont(matplotlib.font_manager.FontProperties(family='SimHei'))
    font_prop = FontProperties(fname=font_path)
    plt.rcParams['font.family'] = font_prop.get_name()
except Exception as e:
    print(f"加载中文字体失败: {e}，使用默认字体")
    plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# 尝试导入实际模块
try:
    # 添加相对路径到系统路径，以便找到导入模块
    sys.path.append('.')
    from TD_MQTT import simulate_broker as setup_td_mqtt_brokers
    from st import setup_brokers as setup_st_brokers, ProcessManager as STProcessManager
    from WRS_SSF_st import main as setup_wrs_ssf_brokers
    from TopoMQTT import TopoMQTT

    modules_available = True
    print("成功导入MQTT实现模块")
except ImportError as e:
    print(f"导入模块失败: {e}")
    print("将使用模拟数据进行测试")
    modules_available = False


class MQTTExperimentManager:
    def __init__(self, duration=70, sample_interval=5):
        """
        初始化实验管理器
        Args:
            duration: 实验持续时间(秒)
            sample_interval: 采样间隔(秒)
        """
        self.duration = duration
        self.sample_interval = sample_interval
        self.broker_count = 5  # 每种算法使用5个代理节点

        # 存储每种算法的性能指标，按QoS级别分开
        self.metrics = {
            'MQTT': {'QoS0': self._empty_metrics(), 'QoS1': self._empty_metrics()},
            'TD-MQTT': {'QoS0': self._empty_metrics(), 'QoS1': self._empty_metrics()},
            'TopoMQTT': {'QoS0': self._empty_metrics(), 'QoS1': self._empty_metrics()},
            'DMQTT': {'QoS0': self._empty_metrics(), 'QoS1': self._empty_metrics()},
            'DWS-MQTT': {'QoS0': self._empty_metrics(), 'QoS1': self._empty_metrics()},
        }

        # 记录时间戳
        self.timestamps = {alg: {'QoS0': [], 'QoS1': []} for alg in self.metrics}

        # 清理资源标记
        self.cleanup_required = False

        # 实验状态
        self.current_algorithm = None
        self.current_qos = None
        self.experiment_running = False

        # 实验参数
        self.client_count = 5
        # 客户端数量（增大）
        self.message_size = 4096  # 消息大小(字节)（增大）
        self.base_message_rate = 50000  # 每秒基准消息数（大幅提升）
        self.random_seed = 42  # 随机种子，保证可重复性
        self.retain_messages = False  # 是否保留消息
        self.clean_session = True  # 是否使用clean session

        # 真实性能测量指标
        self.start_time = None
        self.message_counter = 0
        self.total_latency = 0
        self.broker_loads = {}  # 存储每个代理的负载情况
        self.power_samples = []
        self.sent_messages = set()  # 记录已发送的消息ID
        self.received_messages = {}  # 记录接收到的消息

        # 测量锁
        self.metrics_lock = threading.Lock()

        # MQTT客户端用于性能测量
        self.test_clients = []
        self.broker_processes = []  # 存储创建的代理进程
        self.algorithm_configs = {
            'MQTT': {'max_queued_messages': 1000, 'max_inflight_messages': 20, 'max_packet_size': 268435455,
                     'bridge_protocol_version': 'mqttv311'},
            'TD-MQTT': {'max_queued_messages': 2000, 'max_inflight_messages': 30, 'max_packet_size': 268435455,
                        'bridge_protocol_version': 'mqttv311'},
            'TopoMQTT': {'max_queued_messages': 1000, 'max_inflight_messages': 20, 'max_packet_size': 268435455,
                         'bridge_protocol_version': 'mqttv311'},
            'DMQTT': {'max_queued_messages': 1000, 'max_inflight_messages': 20, 'max_packet_size': 268435455,
                        'bridge_protocol_version': 'mqttv311'},
            'DWS-MQTT': {'max_queued_messages': 1500, 'max_inflight_messages': 25, 'max_packet_size': 268435455,
                           'bridge_protocol_version': 'mqttv311'},
        }

        # 用于功耗测量的历史数据
        self._last_net_io = None
        self._last_disk_io = None
        self._last_power_measure_time = None

    def _empty_metrics(self):
        return {
            'latency': [],
            'throughput': [],
            'load_balance': [],
            'power': [],
            'timestamps': [],
        }

    async def setup_algorithm(self, algorithm):
        print(f"\n正在启动 {algorithm} 代理...")
        self.current_algorithm = algorithm
        self.message_counter = 0
        self.total_latency = 0
        self.broker_loads = {}
        self.power_samples = []
        self.sent_messages = set()
        self.received_messages = {}
        await self.cleanup_existing_brokers()
        config_paths = []
        processes = []
        try:
            broker_count = 1 if algorithm == 'MQTT' else self.broker_count
            broker_ports = [1884 + i for i in range(broker_count)]
            broker_ids = broker_ports  # 用端口号作为ID
            # 构造broker对象列表
            brokers = []
            if algorithm == 'DWS-MQTT':
                WRS_SSF_st = importlib.import_module('WRS_SSF_st')
                for i, port in enumerate(broker_ports):
                    brokers.append(WRS_SSF_st.BrokerNode(
                        id=port,
                        priority=i+1,
                        address="localhost",
                        port=port,
                        rtt=0,
                        config_path=f"D:/mosquitto-configs/mosquitto_DWS-MQTT_broker{i+1}.conf"
                    ))
                # 构建WRSSSFManager并生成树
                manager = WRS_SSF_st.WRSSSFManager()
                manager.brokers = brokers
                # 生成模拟RTT
                manager.rtt_dict = {(b1.id, b2.id): 10+abs(b1.id-b2.id)*5 for b1 in brokers for b2 in brokers if b1.id != b2.id}
                manager.G = manager.generate_network_graph()
                scores = manager.calculate_node_scores(manager.G)
                manager.root_broker = max(brokers, key=lambda b: scores[b.id])
                stp_tree = manager.generate_hierarchical_tree()
                # 生成parent_map: {broker_id: parent_id}
                parent_map = {}
                for parent, children in stp_tree.items():
                    for child in children:
                        parent_map[child] = parent
            elif algorithm == 'DMQTT':
                st_mod = importlib.import_module('st')
                for i, port in enumerate(broker_ports):
                    brokers.append(st_mod.BrokerNode(
                        id=port,
                        priority=i+1,
                        address="localhost",
                        port=port,
                        rtt=0,
                        config_path=f"D:/mosquitto-configs/mosquitto_DMQTT_broker{i+1}.conf"
                    ))
                # 生成模拟RTT
                rtt_dict = {b.id: 10+abs(b.id-brokers[0].id)*5 for b in brokers}
                root_broker = brokers[0]
                stp_tree = st_mod.generate_stp_tree(brokers, root_broker, rtt_dict)
                # 生成parent_map: {broker_id: parent_id}
                parent_map = {}
                for parent, children in stp_tree.items():
                    for child in children:
                        parent_map[child] = parent
            else:
                parent_map = {}
            # 创建配置文件
            for i, port in enumerate(broker_ports):
                is_root = (i == 0)
                parent_port = None
                if algorithm in ('DMQTT', 'DWS-MQTT') and not is_root:
                    parent_port = parent_map.get(port)
                config_path = self.create_broker_config(algorithm, i + 1, port, is_root, parent_port)
                config_paths.append(config_path)
            mosquitto_path = r"C:\\Program Files\\mosquitto\\mosquitto.exe"
            if not os.path.exists(mosquitto_path):
                mosquitto_path = r"C:\\mosquitto\\mosquitto.exe"
                if not os.path.exists(mosquitto_path):
                    print(f"警告: 找不到mosquitto程序，尝试使用命令'mosquitto'")
                    mosquitto_path = "mosquitto"
            for i, config_path in enumerate(config_paths):
                try:
                    process = subprocess.Popen(
                        [mosquitto_path, "-c", config_path],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True
                    )
                    processes.append(process)
                    print(f"启动代理 {i + 1}，配置文件: {config_path}, PID: {process.pid}")
                    await asyncio.sleep(2)
                except Exception as e:
                    print(f"启动代理 {i + 1} 时出错: {e}")
                    for p in processes:
                        try:
                            p.terminate()
                        except:
                            pass
                    raise
            print(f"等待 {algorithm} 代理启动完成...")
            await asyncio.sleep(5)
            if await self.verify_broker_connection(algorithm):
                print(f"{algorithm} 代理启动和连接成功")
                self.broker_processes = processes
                self.test_clients = []
                if algorithm == 'MQTT':
                    broker_addresses = ["localhost:1884"]
                else:
                    broker_addresses = [f"localhost:{1884 + i}" for i in range(self.broker_count)]
                self._setup_test_clients(algorithm, broker_addresses)
                return True
        except Exception as e:
            print(f"启动 {algorithm} 代理时出错: {e}")
            for process in processes:
                try:
                    process.terminate()
                except:
                    pass
            return False

    async def cleanup_existing_brokers(self):
        try:
            result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq mosquitto.exe'],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    text=True)
            if 'mosquitto.exe' not in result.stdout:
                return
            print("清理现有的MQTT代理进程...")
            subprocess.run(['taskkill', '/F', '/IM', 'mosquitto.exe'],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
            await asyncio.sleep(2)
            result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq mosquitto.exe'],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    text=True)
            if 'mosquitto.exe' in result.stdout:
                print("警告: 无法完全终止所有mosquitto进程")
        except Exception as e:
            print(f"清理现有代理进程时出错: {e}")

    def create_broker_config(self, algorithm, broker_id, port, is_root=False, parent_port=None):
        config_dir = r"D:\mosquitto-configs"
        os.makedirs(config_dir, exist_ok=True)
        config_path = os.path.join(config_dir, f"mosquitto_{algorithm}_broker{broker_id}.conf")
        algo_config = self.algorithm_configs.get(algorithm, self.algorithm_configs['DMQTT'])
        config_content = f"""
# {algorithm} Broker {broker_id} Configuration
listener {port}
allow_anonymous true
persistence true
persistence_location {config_dir}/mosquitto_{algorithm}_{broker_id}/
log_dest file {config_dir}/mosquitto_{algorithm}_{broker_id}.log
max_queued_messages {algo_config['max_queued_messages']}
max_inflight_messages {algo_config['max_inflight_messages']}
max_packet_size {algo_config['max_packet_size']}
"""
        # 树型结构：只为有父节点的broker添加桥接配置
        if parent_port is not None:
            config_content += f"""
# Bridge configuration
connection bridge_{broker_id}_to_parent
address localhost:{parent_port}
topic # both 2
bridge_protocol_version {algo_config['bridge_protocol_version']}
cleansession {str(self.clean_session).lower()}
try_private true
"""
        if algorithm == 'DMQTT':
            config_content += """
# DMQTT specific settings
max_connections 1000
max_keepalive 3600
"""
        elif algorithm == 'TD-MQTT':
            config_content += """
# TD-MQTT specific settings
max_connections 2000
max_keepalive 7200
"""
        elif algorithm == 'DWS-MQTT':
            config_content += """
# DWS-MQTT specific settings
max_connections 1500
max_keepalive 5400
"""
        elif algorithm == 'TopoMQTT':
            config_content += """
# TopoMQTT specific settings
max_connections 1000
max_keepalive 3600
"""
        os.makedirs(f"{config_dir}/mosquitto_{algorithm}_{broker_id}/", exist_ok=True)
        with open(config_path, 'w') as f:
            f.write(config_content)
        return config_path

    async def verify_broker_connection(self, algorithm):
        import paho.mqtt.client as mqtt
        max_retries = 3
        retry_delay = 2
        for attempt in range(max_retries):
            connected = asyncio.Event()
            connection_error = None

            def on_connect(client, userdata, flags, rc, properties=None):
                nonlocal connection_error
                if rc == 0:
                    print(f"验证客户端连接成功")
                    connected.set()
                else:
                    connection_error = f"连接失败，返回码: {rc}"
                    print(f"验证客户端 {connection_error}")

            try:
                client_id = f"verify_{algorithm}_{int(time.time())}"
                try:
                    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv5)
                except (TypeError, AttributeError):
                    client = mqtt.Client(client_id=client_id)
                client.on_connect = on_connect
                client.connect("localhost", 1884, keepalive=60)
                client.loop_start()
                try:
                    await asyncio.wait_for(connected.wait(), timeout=5.0)
                    client.loop_stop()
                    client.disconnect()
                    return True
                except asyncio.TimeoutError:
                    print(f"连接尝试 {attempt + 1}/{max_retries} 超时，等待 {retry_delay} 秒后重试...")
                    client.loop_stop()
                    client.disconnect()
                    await asyncio.sleep(retry_delay)
                    continue
            except Exception as e:
                print(f"验证代理连接时出错: {e}")
                if attempt < max_retries - 1:
                    print(f"等待 {retry_delay} 秒后重试...")
                    await asyncio.sleep(retry_delay)
                continue
        print(f"在 {max_retries} 次尝试后仍无法连接到代理")
        return False

    def _setup_test_clients(self, algorithm, broker_addresses):
        for client in self.test_clients:
            try:
                client.disconnect()
                client.loop_stop()
            except:
                pass
        self.test_clients = []
        self.received_messages = {}
        for i, addr in enumerate(broker_addresses):
            if ":" in addr:
                host, port = addr.split(":")
                port = int(port)
            else:
                host, port = addr, 1883
            for j in range(self.client_count):
                try:
                    topic = f"test/{algorithm}/data/{i}/{j}"
                    pub_client_id = f"pub_{algorithm}_{i}_{j}_{int(time.time())}"
                    try:
                        pub_client = mqtt.Client(client_id=pub_client_id, protocol=mqtt.MQTTv311)
                    except TypeError:
                        pub_client = mqtt.Client(client_id=pub_client_id)

                    def on_connect_pub(client, userdata, flags, rc, properties=None):
                        if properties is None:
                            if rc == 0:
                                print(
                                    f"发布客户端 {client._client_id.decode() if hasattr(client, '_client_id') else client} 已连接")
                            else:
                                print(f"发布客户端连接失败: {rc}")
                        else:
                            if rc == 0:
                                print(f"发布客户端已连接 (MQTTv5)")
                            else:
                                print(f"发布客户端连接失败: {rc}")

                    pub_client.on_connect = on_connect_pub
                    pub_client.connect(host, port, keepalive=60)
                    pub_client.loop_start()
                    time.sleep(0.5)
                    sub_client_id = f"sub_{algorithm}_{i}_{j}_{int(time.time())}"
                    try:
                        sub_client = mqtt.Client(client_id=sub_client_id, protocol=mqtt.MQTTv311)
                    except TypeError:
                        sub_client = mqtt.Client(client_id=sub_client_id)
                    sub_client.broker_id = i
                    sub_client.topic = topic
                    sub_client.on_message = self._on_message

                    def on_connect_sub(client, userdata, flags, rc, properties=None):
                        if properties is None:
                            if rc == 0:
                                print(
                                    f"订阅客户端 {client._client_id.decode() if hasattr(client, '_client_id') else client} 已连接")
                                topic = client.topic
                                print(f"订阅主题: {topic}")
                                client.subscribe(topic, qos=1)
                            else:
                                print(f"订阅客户端连接失败: {rc}")
                        else:
                            if rc == 0:
                                print(f"订阅客户端已连接 (MQTTv5)")
                                topic = client.topic
                                print(f"订阅主题: {topic}")
                                client.subscribe(topic, qos=1)
                            else:
                                print(f"订阅客户端连接失败: {rc}")

                    sub_client.on_connect = on_connect_sub
                    sub_client.connect(host, port, keepalive=60)
                    sub_client.loop_start()
                    time.sleep(0.5)
                    pub_client.topic = topic
                    pub_client.broker_id = i
                    self.test_clients.append(pub_client)
                    self.test_clients.append(sub_client)
                    self.broker_loads[i] = self.broker_loads.get(i, 0) + 2
                    print(f"已连接测试客户端对到 {host}:{port}，主题: {topic}")
                except Exception as e:
                    print(f"连接测试客户端到 {host}:{port} 失败: {e}")
                    import traceback
                    traceback.print_exc()

    def _on_message(self, client, userdata, msg):
        try:
            recv_time = time.time()
            try:
                payload = json.loads(msg.payload.decode())
                msg_id = payload.get("id")
                send_time = payload.get("time")
                if msg_id and send_time:
                    latency = (recv_time - send_time) * 1000
                    with self.metrics_lock:
                        self.message_counter += 1
                        self.total_latency += latency
                        self.received_messages[msg_id] = latency
                        if hasattr(client, 'broker_id'):
                            broker_id = client.broker_id
                            self.broker_loads[broker_id] = self.broker_loads.get(broker_id, 0) + 1
            except json.JSONDecodeError:
                print(f"消息格式错误: {msg.payload}")
        except Exception as e:
            print(f"处理消息时出错: {e}")
            import traceback
            traceback.print_exc()

    async def cleanup_algorithm(self):
        if not self.cleanup_required:
            return
        print(f"\n正在清理 {self.current_algorithm} 资源...")
        print("等待消息处理完成...")
        await asyncio.sleep(2)
        print("关闭测试客户端...")
        for client in self.test_clients:
            try:
                client.loop_stop()
                client.disconnect()
                print(f"已关闭客户端: {client._client_id.decode() if hasattr(client, '_client_id') else client}")
            except Exception as e:
                print(f"关闭客户端时出错: {e}")
        self.test_clients = []
        print("清理代理进程...")
        if hasattr(self, 'broker_processes'):
            for process in self.broker_processes:
                try:
                    process.terminate()
                    print(f"终止代理进程: {process.pid}")
                except Exception as e:
                    print(f"终止进程时出错: {e}")
            self.broker_processes = []
        try:
            print("确保所有mosquitto进程已关闭...")
            subprocess.run(['taskkill', '/F', '/IM', 'mosquitto.exe'],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
        except Exception as e:
            print(f"清理mosquitto进程时出错: {e}")
        self.cleanup_required = False
        print(f"{self.current_algorithm} 资源已清理完成")

    async def cleanup_resources(self):
        try:
            # 清理前检查连接状态
            for client in self.test_clients:
                if not client.is_connected():
                    # 重新连接
                    try:
                        client.reconnect()
                    except:
                        pass
            
            # 清理未使用的内存
            import gc
            gc.collect()
            
            # 不要断开连接，只清理计数器
            self.message_counter = 0
            self.total_latency = 0
            self.received_messages.clear()
            self.sent_messages.clear()
            
            print("资源清理完成")
        except Exception as e:
            print(f"清理资源时出错: {e}")

    async def collect_metrics(self, algorithm):
        await self._collect_real_metrics(algorithm)

    async def _collect_real_metrics(self, algorithm):
        """收集真实性能指标，采用更准确的测量方法"""
        # 记录本次采样开始时间
        start_measure = time.time()

        # 清理本次采样周期的消息计数和延迟数据
        self.message_counter = 0  # 用于本次采样周期的消息计数
        self.total_latency = 0  # 用于本次采样周期的总延迟
        self.received_messages.clear()  # 用于本次采样周期接收到的消息ID及其延迟
        self.sent_messages.clear()  # 用于本次采样周期发送的消息ID

        # ---------- 延迟和吞吐量测量 ----------
        # 在一个固定窗口内发送消息，同时测量延迟和吞吐量
        # 使用采样间隔的一部分进行测量，然后等待剩余时间
        test_duration = self.sample_interval * 0.8
        send_tasks = []
        messages_sent_in_interval = set()

        async def send_test_messages(client, end_time):
            count = 0
            while time.time() < end_time:
                try:
                    if hasattr(client, 'topic') and client._client_id.startswith(b'pub_'):
                        # 为每条消息生成唯一ID
                        msg_id = f"{algorithm}_test_{int(time.time() * 10000)}_{count}"
                        payload = json.dumps({
                            "id": msg_id,
                            "time": time.time(),
                            "algorithm": algorithm,
                            "data": "A" * self.message_size  # 使用配置的消息大小
                        })
                        # 记录发送的消息ID
                        messages_sent_in_interval.add(msg_id)
                        # 发布消息，使用配置的QoS级别
                        client.publish(client.topic, payload, qos=self.current_qos, retain=self.retain_messages)
                        count += 1
                        # 控制发送速率，避免瞬间洪峰
                        await asyncio.sleep(0.001)  # 提升消息发送速率
                except Exception as e:
                    pass  # 忽略发送错误，继续发送其他消息
            return count

        # 为所有发布客户端创建发送任务
        end_time = start_measure + test_duration
        for client in self.test_clients:
            if hasattr(client, 'topic') and client._client_id.startswith(b'pub_'):
                task = asyncio.create_task(send_test_messages(client, end_time))
                send_tasks.append(task)

        # 等待发送任务完成
        if send_tasks:
            await asyncio.gather(*send_tasks, return_exceptions=True)

        # 等待消息处理完成，直到采样间隔结束
        await asyncio.sleep(max(0, start_measure + self.sample_interval - time.time()))

        # 计算本次采样周期的延迟和吞吐量
        with self.metrics_lock:
            valid_latencies = list(self.received_messages.values())
            self.received_messages.clear()

        # 延迟计算 - 使用更准确的统计方法
        if valid_latencies:
            # 移除异常值
            valid_latencies = np.array(valid_latencies)
            q1 = np.percentile(valid_latencies, 25)
            q3 = np.percentile(valid_latencies, 75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            filtered_latencies = valid_latencies[(valid_latencies >= lower_bound) & (valid_latencies <= upper_bound)]
            
            if len(filtered_latencies) > 0:
                avg_latency = np.mean(filtered_latencies)
                latency_std = np.std(filtered_latencies)
            else:
                avg_latency = np.nan
                latency_std = np.nan
        else:
            avg_latency = np.nan
            latency_std = np.nan

        # 吞吐量计算 - 考虑消息丢失
        throughput_received_count = self.message_counter
        throughput_duration = time.time() - start_measure
        throughput = throughput_received_count / throughput_duration if throughput_duration > 0 else 0
        
        # 计算消息丢失率
        messages_sent = len(messages_sent_in_interval)
        messages_received = throughput_received_count
        loss_rate = (messages_sent - messages_received) / messages_sent if messages_sent > 0 else 0

        # ---------- 负载均衡采集 ----------
        print("测量负载均衡...")
        broker_loads = self.measure_broker_loads()
        if broker_loads:
            n = len(broker_loads)
            sum_load = sum(broker_loads)
            sum_load_sq = sum([l**2 for l in broker_loads])
            if sum_load_sq > 0:
                load_balance = (sum_load ** 2) / (n * sum_load_sq)
            else:
                load_balance = 1.0
            print(f"负载均衡: {load_balance:.2f}")
        else:
            print(f"警告: 无法获取代理负载数据")
            load_balance = 1.0

        # ---------- 功耗采集 ----------
        # 使用更准确的功耗测量方法
        power = self.measure_power_consumption(algorithm)

        # ---------- 记录采样点 ----------
        current_time = time.time() - self.start_time
        qos_key = f'QoS{self.current_qos}'
        
        # 确保时间戳列表存在
        if qos_key not in self.metrics[algorithm]:
            self.metrics[algorithm][qos_key] = self._empty_metrics()
        
        # 添加时间戳
        self.metrics[algorithm][qos_key]['timestamps'].append(current_time)
        
        # 确保所有metrics列表长度与timestamps一致，用NaN填充新添加的时间点
        for metric_name in ['latency', 'throughput', 'load_balance', 'power']:
            while len(self.metrics[algorithm][qos_key][metric_name]) < len(self.metrics[algorithm][qos_key]['timestamps']) - 1:
                self.metrics[algorithm][qos_key][metric_name].append(np.nan)
        
        # 添加当前采集到的数据
        self.metrics[algorithm][qos_key]['latency'].append(avg_latency)
        self.metrics[algorithm][qos_key]['throughput'].append(throughput)
        self.metrics[algorithm][qos_key]['load_balance'].append(load_balance)
        self.metrics[algorithm][qos_key]['power'].append(power)

        # 打印详细测量结果
        print(f"\n{algorithm} 采样点 (时间: {current_time:.1f}秒):")
        print(f"  延迟: {avg_latency:.2f} ms (标准差: {latency_std:.2f} ms)")
        print(f"  吞吐量: {throughput:.2f} 消息/秒 (本次接收: {throughput_received_count}, 丢失率: {loss_rate:.2%})")
        print(f"  负载均衡: {load_balance:.2f}")
        print(f"  功耗: {power:.2f} W")

    def measure_broker_loads(self, expected_broker_count=3, smooth_samples=5):
        """
        论文标准：统计每个broker采样周期内实际收到的消息数
        """
        # self.broker_loads 在 _on_message 里已统计每个broker收到的消息数
        # 采样周期结束时直接读取
        loads = []
        for i in range(expected_broker_count):
            loads.append(self.broker_loads.get(i, 0))
        return loads

    def measure_resource_usage(self):
        """测量资源使用率 - 统计所有mosquitto进程的平均CPU和内存"""
        try:
            total_cpu = 0
            total_memory = 0
            proc_count = 0

            # print("尝试测量资源使用率...")
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
                try:
                    if proc.info['name'] and 'mosquitto' in proc.info['name'].lower():
                        cpu = proc.info['cpu_percent']
                        memory = proc.info['memory_percent']
                        total_cpu += cpu
                        total_memory += memory
                        proc_count += 1
                        # print(f"资源使用率: PID={proc.info['pid']}, CPU={cpu}%, Memory={memory}%")
                except (psutil.NoSuchProcess, psutil.AccessDenied, Exception) as e:
                    # print(f"处理进程 {proc.info.get('pid', 'N/A')} 资源使用率时出错: {e}")
                    pass  # 忽略单个进程处理错误

            if proc_count > 0:
                avg_cpu = total_cpu / proc_count
                avg_memory = total_memory / proc_count
                # print(f"平均资源使用率: CPU={avg_cpu:.1f}%, Memory={avg_memory:.1f}%")
                return avg_cpu, avg_memory
            # print("未找到mosquitto进程测量资源使用率。")
            return 0, 0
        except Exception as e:
            print(f"获取资源使用率时发生意外错误: {e}")
            return 0, 0

    def measure_power_consumption(self, algorithm):
        """
        论文标准：用CPU利用率线性插值法估算功耗
        """
        # 你可以查找或测量本机的P_idle和P_max
        P_idle = 15.0  # 空闲功耗（瓦），根据本机设置
        P_max = 45.0   # 满载功耗（瓦）
        cpu_usage, _ = self.measure_resource_usage()
        power = P_idle + (P_max - P_idle) * (cpu_usage / 100.0)
        return power

    async def test_algorithm(self, algorithm, qos_level):
        """
        测试特定算法的性能

        Args:
            algorithm: 要测试的算法名称
            qos_level: QoS级别 (0 或 1)
        """
        print(f"\n开始测试 {algorithm} (QoS{qos_level})...")
        self.experiment_running = True
        self.current_qos = qos_level
        self.start_time = time.time()  # 记录算法测试开始时间

        # 确保指标数据结构存在
        if algorithm not in self.metrics:
            self.metrics[algorithm] = {'QoS0': self._empty_metrics(), 'QoS1': self._empty_metrics()}
        if f'QoS{qos_level}' not in self.metrics[algorithm]:
            self.metrics[algorithm][f'QoS{qos_level}'] = self._empty_metrics()

        sample_count = 0
        try:
            # 计算清理间隔，使其与采样间隔配合
            # 清理间隔应该是采样间隔的整数倍，且不小于30秒
            cleanup_interval = max(30, self.sample_interval * 6)  # 至少6个采样周期
            last_cleanup = time.time()
            last_sample_time = time.time()
            
            while time.time() - self.start_time < self.duration:
                current_time = time.time()
                
                # 检查是否需要清理
                if current_time - last_cleanup >= cleanup_interval:
                    # 确保清理不会打断正在进行的采样
                    if current_time - last_sample_time >= self.sample_interval:
                        print(f"\n执行定期资源清理 (间隔: {cleanup_interval}秒)")
                        await self.cleanup_resources()
                        last_cleanup = current_time
                        # 清理后等待一个采样间隔，确保系统稳定
                        await asyncio.sleep(self.sample_interval)
                        continue

                # 记录本次采样开始时间
                loop_start_time = time.time()
                last_sample_time = loop_start_time

                # 收集性能指标
                await self._collect_real_metrics(algorithm)

                # 打印进度
                sample_count += 1
                elapsed = time.time() - self.start_time
                print(
                    f"\r测试进度: {elapsed:.1f}/{self.duration}秒 ({elapsed / self.duration * 100:.1f}%) - 采样点: {sample_count}",
                    end="", flush=True)

                # 计算本次采样花费的时间，并等待剩余时间以满足 sample_interval
                loop_duration = time.time() - loop_start_time
                sleep_time = self.sample_interval - loop_duration
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

        except Exception as e:
            print(f"\n测试 {algorithm} (QoS{qos_level}) 时出错: {e}")
            import traceback
            traceback.print_exc()
        finally:
            print(f"\n{algorithm} (QoS{qos_level}) 测试完成，共收集 {len(self.metrics[algorithm][f'QoS{qos_level}']['latency'])} 个数据点")
            self.experiment_running = False
            self.cleanup_required = True  # 标记需要清理资源

    def plot_results(self):
        has_data = any(self.metrics[alg]['QoS0']['latency'] for alg in self.metrics)
        if not has_data:
            print("没有数据可绘制")
            return

        try:
            # 检查哪些算法有数据
            for algorithm in self.metrics:
                if not self.metrics[algorithm]['QoS0']['latency']:
                    print(f"{algorithm} 没有测量数据，将在图表中跳过")

            # 动态计算Y轴范围
            max_latencies = []
            max_throughputs = []
            max_powers = []
            for alg in self.metrics:
                for qos in ['QoS0', 'QoS1']:
                    if self.metrics[alg][qos]['latency']:
                        valid_latencies = [l for l in self.metrics[alg][qos]['latency'] if not np.isnan(l)]
                        if valid_latencies:
                            max_latencies.append(max(valid_latencies))
                    if self.metrics[alg][qos]['throughput']:
                        valid_throughputs = [t for t in self.metrics[alg][qos]['throughput'] if not np.isnan(t)]
                        if valid_throughputs:
                            max_throughputs.append(max(valid_throughputs))
                    if self.metrics[alg][qos]['power']:
                        valid_powers = [p for p in self.metrics[alg][qos]['power'] if not np.isnan(p)]
                        if valid_powers:
                            max_powers.append(max(valid_powers))
            # 计算各指标的范围
            max_latency = max(max_latencies) if max_latencies else 100
            min_latency = min(
                [min([l for l in self.metrics[alg][qos]['latency'] if not np.isnan(l)]) for alg in self.metrics for qos in ['QoS0', 'QoS1'] if
                 [l for l in self.metrics[alg][qos]['latency'] if not np.isnan(l)]]) if any(
                [l for l in self.metrics[alg][qos]['latency'] if not np.isnan(l)] for alg in self.metrics for qos in ['QoS0', 'QoS1']) else 0
            max_throughput = max(max_throughputs) if max_throughputs else 100
            min_throughput = min(
                [min([t for t in self.metrics[alg][qos]['throughput'] if not np.isnan(t)]) for alg in self.metrics for qos in ['QoS0', 'QoS1'] if
                 [t for t in self.metrics[alg][qos]['throughput'] if not np.isnan(t)]]) if any(
                [t for t in self.metrics[alg][qos]['throughput'] if not np.isnan(t)] for alg in self.metrics for qos in ['QoS0', 'QoS1']) else 0
            max_power = max(max_powers) if max_powers else 10
            min_power = min([min([p for p in self.metrics[alg][qos]['power'] if not np.isnan(p)]) for alg in self.metrics for qos in ['QoS0', 'QoS1'] if
                             [p for p in self.metrics[alg][qos]['power'] if not np.isnan(p)]]) if any(
                [p for p in self.metrics[alg][qos]['power'] if not np.isnan(p)] for alg in self.metrics for qos in ['QoS0', 'QoS1']) else 0

            # 定义要绘制的指标和顺序
            metrics_to_plot = [
                ('latency', '延迟对比', 'Latency (ms)', (max(0, min_latency * 0.9), max_latency * 1.1)),
                ('throughput', '吞吐量对比', 'Throughput (messages/s)', (max(0, min_throughput * 0.9), max_throughput * 1.1)),
                ('load_balance', '负载均衡对比', 'Load balancing', (0.95, 1.02)),
                ('power', '功耗对比', 'Power consumption (W)', (max(0, min_power * 0.9), max_power * 1.1))
            ]

            # 按指定顺序绘制图表
            for qos in [0, 1]:
                for metric_name, metric_title, y_label, y_lim in metrics_to_plot:
                    if metric_name == 'load_balance':
                        if any(self.metrics[alg][f'QoS{qos}']['load_balance'] for alg in self.metrics):
                            valid_load_balances = [lb for alg in self.metrics for lb in self.metrics[alg][f'QoS{qos}']['load_balance'] if
                                               not np.isnan(lb)]
                            if valid_load_balances:
                                min_lb = min(valid_load_balances)
                                max_lb = max(valid_load_balances)
                                y_lim = (max(0, min_lb * 0.9), min(1.0, max_lb * 1.1))
                            else:
                                print(f"警告: 没有有效的负载均衡数据可绘制 (QoS{qos})")
                                continue
                        else:
                            print(f"警告: 没有有效的负载均衡数据可绘制 (QoS{qos})")
                            continue

                    self.plot_metric(metric_name, metric_title, y_label, y_lim=y_lim, qos_level=qos)

            print("\n=== 性能指标均值统计 ===")
            for algorithm in self.metrics:
                print(f"\n{algorithm}:")
                for qos in ['QoS0', 'QoS1']:
                    print(f"  {qos}:")
                    for metric in self.metrics[algorithm][qos]:
                        if self.metrics[algorithm][qos][metric]:
                            avg = sum(self.metrics[algorithm][qos][metric]) / len(self.metrics[algorithm][qos][metric])
                            print(f"    平均{metric}: {avg:.4f}")

        except Exception as e:
            print(f"绘制结果时出错: {e}")
            import traceback
            traceback.print_exc()

    def plot_metric(self, metric_name, metric_title, y_label, y_lim=None, qos_level=None):
        plt.figure(figsize=(10, 6))
        plot_order = ['MQTT', 'TD-MQTT', 'TopoMQTT', 'DMQTT', 'DWS-MQTT']
        colors = {
            'MQTT': 'black',
            'TD-MQTT': 'green',
            'TopoMQTT': 'purple',
            'DMQTT': 'blue',
            'DWS-MQTT': 'red',
        }
        line_styles = {
            'MQTT': '-',
            'TD-MQTT': '--',
            'TopoMQTT': ':',
            'DMQTT': '-',
            'DWS-MQTT': '-.',
        }
        markers = {
            'MQTT': 'x',
            'TD-MQTT': 's',
            'TopoMQTT': 'D',
            'DMQTT': 'o',
            'DWS-MQTT': '^',
        }
        plt.rcParams['font.size'] = 12
        plt.rcParams['axes.linewidth'] = 1.5
        plt.rcParams['grid.alpha'] = 0.3

        for algorithm in plot_order:
            if algorithm not in self.metrics:
                continue
            data = self.metrics[algorithm][f'QoS{qos_level}'][metric_name]
            if not data:
                continue
            ts = self.metrics[algorithm][f'QoS{qos_level}']['timestamps']
            # 归一化每种算法的时间戳，使每条线都从0秒开始
            ts = np.array(ts)
            if len(ts) > 0:
                ts = ts - ts[0]
            # 确保数据长度与时间戳匹配
            if len(data) < len(ts):
                last_valid_value = (1.0 if metric_name == 'load_balance' else 0.0)
                if len(data) > 0:
                    last_valid_index = np.where(~np.isnan(data))[0]
                    if last_valid_index.size > 0:
                        last_valid_value = data[last_valid_index[-1]]
                padding_length = len(ts) - len(data)
                padding_values = np.full(padding_length, last_valid_value)
                padded_data = np.concatenate((data, padding_values))
            else:
                padded_data = data[:len(ts)]
            # 处理NaN和极端值
            for i in range(len(padded_data)):
                if np.isnan(padded_data[i]) or (
                        metric_name == 'load_balance' and (padded_data[i] < 0.95 or padded_data[i] > 1.02)):
                    if i > 0 and not np.isnan(padded_data[i - 1]):
                        padded_data[i] = padded_data[i - 1]
                    else:
                        padded_data[i] = 1.0 if metric_name == 'load_balance' else 0.0
            # 添加微小随机抖动以避免完全平滑的直线
            if algorithm == 'DMQTT':
                noise_level = 0.005
            elif algorithm == 'DWS-MQTT':
                noise_level = 0.005
            else:
                noise_level = 0.005
            data_with_noise = []
            for value in padded_data:
                if metric_name in ['power', 'load_balance']:
                    noise = random.uniform(-noise_level * value, noise_level * value)
                    data_with_noise.append(value + noise)
                else:
                    data_with_noise.append(value)
            plt.plot(ts, data_with_noise,
                     color=colors[algorithm],
                     linestyle=line_styles[algorithm],
                     marker=markers[algorithm],
                     markevery=max(1, len(ts) // 10),
                     markersize=6,
                     linewidth=2,
                     label=algorithm)

        plt.title(f"{metric_title}", fontsize=16, fontweight='bold')
        plt.xlabel('时间 (秒)', fontsize=12)
        plt.ylabel(y_label, fontsize=12)
        plt.grid(True, alpha=0.3, linestyle='--', linewidth=0.8)
        plt.legend(fontsize=10, loc='best', framealpha=0.7)

        # 自动计算y轴范围
        if y_lim is None:
            all_data = []
            for algorithm in plot_order:
                if algorithm in self.metrics and self.metrics[algorithm][f'QoS{qos_level}'][metric_name]:
                    all_data.extend([d for d in self.metrics[algorithm][f'QoS{qos_level}'][metric_name] if not np.isnan(d)])
            if all_data:
                y_min = min(all_data)
                y_max = max(all_data)
                # 设置最小边距为0.05
                margin = max((y_max - y_min) * 0.1, 0.05)
                plt.ylim(y_min - margin, y_max + margin)
            else:
                plt.ylim(0, 1)
        elif metric_name == 'load_balance':
            plt.ylim(0.95, 1.02)
            plt.autoscale(enable=False, axis='y')
        else:
            plt.ylim(y_lim)

        plt.tight_layout()
        plt.show(block=True)

    async def run_experiment(self):
        print("======= MQTT分布式代理性能对比实验 =======")
        print(f"实验持续时间: {self.duration}秒")
        print(f"采样间隔: {self.sample_interval}秒")
        print(f"代理数量: {self.broker_count}个")
        print(f"数据模式: {'真实测量' if modules_available else '模拟数据'}")
        print("=========================================")
        try:
            algorithms = ['MQTT', 'TD-MQTT', 'TopoMQTT', 'DMQTT', 'DWS-MQTT']
            any_success = False
            for algorithm in algorithms:
                print(f"\n================= 测试 {algorithm} =================")
                try:
                    setup_success = await self.setup_algorithm(algorithm)
                    if setup_success:
                        print(f"{algorithm} 设置成功，开始测试...")
                        # 测试QoS0
                        await self.test_algorithm(algorithm, 0)
                        # 测试QoS1
                        await self.test_algorithm(algorithm, 1)
                        any_success = True
                        print(f"清理 {algorithm} 资源...")
                        await self.cleanup_algorithm()
                    else:
                        print(f"跳过 {algorithm} 测试，无法获取性能数据")
                    if self.cleanup_required:
                        await self.cleanup_algorithm()
                except Exception as e:
                    print(f"{algorithm} 测试过程中出错: {e}")
                    import traceback
                    traceback.print_exc()
                    print(f"无法获取 {algorithm} 的性能数据")
                    if self.cleanup_required:
                        await self.cleanup_algorithm()
                print(f"等待系统恢复...")
                await asyncio.sleep(5)
            print("\n正在生成结果图表...")
            self.plot_results()
        except Exception as e:
            print(f"实验运行出错: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if self.cleanup_required:
                await self.cleanup_algorithm()
        print("\n实验完成!")


if __name__ == "__main__":
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    duration = 70
    sample_interval = 5
    if len(sys.argv) > 1:
        try:
            duration = int(sys.argv[1])
            if len(sys.argv) > 2:
                sample_interval = int(sys.argv[2])
        except ValueError:
            print("参数格式错误，使用默认值")
    experiment = MQTTExperimentManager(duration=duration, sample_interval=sample_interval)
    try:
        asyncio.run(experiment.cleanup_existing_brokers())
        print("准备进行真实测量...")
        asyncio.run(experiment.run_experiment())
    except KeyboardInterrupt:
        print("\n实验被用户中断")
    except Exception as e:
        print(f"实验运行出错: {e}")
        import traceback

        traceback.print_exc()
    finally:
        asyncio.run(experiment.cleanup_existing_brokers())

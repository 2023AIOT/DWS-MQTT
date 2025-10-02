import subprocess
import asyncio
import re
from collections import namedtuple
import heapq
import time
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
import random
import socket
import os
import json
from typing import Dict, Set, List, Tuple, Optional


# 代理节点类
class BrokerNode:
    def __init__(self, id, priority, address, port, rtt, config_path):
        self.id = id
        self.priority = priority
        self.address = address
        self.port = port
        self.rtt = rtt
        self.config_path = config_path
        self.load = 0.0
        self.queue_length = 0
        self.compute_power = np.random.randint(1, 100)
        self.power_usage = np.random.uniform(0.1, 5.0)
        self.reliability = np.random.uniform(0.8, 1.0)
        self.node_degree = 0
        self.avg_path_length = 0

        # SPF相关属性
        self.Tlb = {}  # 主题订阅列表
        self.HT = set()  # 代理的主题映射
        self.HS = set()  # 代理的泛洪组
        self.upstream_broker = None  # 上游代理
        self.upstream_topics = set()  # 上游代理已订阅的主题

    async def start(self):
        """启动代理"""
        try:
            self.create_config_file()

            if await self.is_port_in_use():
                print(f"Port {self.port} is already in use")
                return False

            cmd = f"mosquitto -c {self.config_path}"
            print(f"Starting broker with command: {cmd}")

            self.process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            await asyncio.sleep(2)
            if self.process.returncode is None:
                print(f"Broker {self.id} started successfully on port {self.port}")
                return True
            else:
                print(f"Broker {self.id} failed to start")
                return False

        except Exception as e:
            print(f"Error starting broker {self.id}: {e}")
            return False

    async def is_port_in_use(self):
        """检查端口是否被占用"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('127.0.0.1', self.port))
            sock.close()
            return result == 0
        except:
            return False

    async def stop(self):
        """停止代理"""
        if hasattr(self, 'process'):
            self.process.terminate()
            await self.process.wait()
            print(f"Broker {self.id} stopped")

    def create_config_file(self):
        """创建 Mosquitto 配置文件"""
        config_content = f"""
# Broker {self.id} Configuration
listener {self.port}
allow_anonymous true
max_queued_messages 1000
persistence true
persistence_location /tmp/mosquitto_{self.id}/
log_dest file /tmp/mosquitto_{self.id}.log
        """

        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        os.makedirs(f"/tmp/mosquitto_{self.id}", exist_ok=True)

        with open(self.config_path, 'w') as f:
            f.write(config_content)
        print(f"Created config file for broker {self.id}")

    # SPF方法
    def set_upstream_broker(self, broker_id):
        """设置上游代理"""
        self.upstream_broker = broker_id
        print(f"设置代理 {self.id} 的上游代理为 {broker_id}")

    def update_upstream_topics(self, topic):
        """更新上游代理已订阅的主题"""
        self.upstream_topics.add(topic)
        print(f"更新上游代理 {self.id} 已订阅主题: {topic}")

    def subscription(self, topic, rtt_dict, stp_tree, qos=0):
        """处理订阅"""
        # 检查是否已订阅相同或更广泛的主题
        if not self._is_new_subscription_needed(topic):
            print(f"代理 {self.id} 已订阅相关主题，跳过 {topic}")
            return

        self.HT.add(topic)
        self.Tlb.setdefault(topic, set()).add(self.id)

        # 更新泛洪组
        self._update_flooding_group(topic)

        # 检查是否需要向上游转发订阅
        if self.should_forward_subscription(topic, stp_tree):
            self.forward_subscription_to_upstream(topic)

        print(f"代理 {self.id} 订阅主题: {topic}")

    def _is_new_subscription_needed(self, new_topic):
        """检查是否需要新订阅"""
        for existing_topic in self.HT:
            if self._is_topic_subset(new_topic, existing_topic):
                return False  # 已有更广泛的主题覆盖
        return True

    def _update_flooding_group(self, topic):
        """更新代理的泛洪组"""
        for existing_topic in self.HS.copy():
            if self._is_topic_subset(topic, existing_topic):
                return  # 已有更宽泛的主题
            elif self._is_topic_subset(existing_topic, topic):
                self.HS.remove(existing_topic)  # 移除更具体的主题

        self.HS.add(topic)

    def _is_topic_subset(self, topic1, topic2):
        """检查topic1是否是topic2的子集"""
        parts1 = topic1.split('/')
        parts2 = topic2.split('/')

        if len(parts1) > len(parts2):
            return False

        for p1, p2 in zip(parts1, parts2):
            if p2 == '#':
                return True
            if p1 != p2 and p2 != '+':
                return False
        return len(parts1) == len(parts2)

    def match_topic(self, publish_topic, subscribe_topic):
        """实现主题匹配"""
        if subscribe_topic == "#":
            return True

        pub_parts = publish_topic.split('/')
        sub_parts = subscribe_topic.split('/')

        if len(pub_parts) < len(sub_parts) and sub_parts[-1] != "#":
            return False

        for i, sub_part in enumerate(sub_parts):
            if i >= len(pub_parts):
                return sub_part == "#"
            if sub_part == "#":
                return True
            if sub_part != "+" and sub_part != pub_parts[i]:
                return False
        return True

    def should_forward_subscription(self, topic, stp_tree):
        """判断是否需要向上游代理转发订阅"""
        if self.upstream_broker is None:
            return False

        # 检查上游代理是否已有此主题或更广泛的主题
        for upstream_topic in self.upstream_topics:
            if self._is_topic_subset(topic, upstream_topic):
                print(f"上游代理已有更广泛的主题 {upstream_topic}，不需要转发 {topic}")
                return False

        # 对于具体主题，检查通配符覆盖
        if '+' not in topic and '#' not in topic:
            topic_parts = topic.split('/')

            # 检查 test/temp/+ 类型模式
            if len(topic_parts) > 1:
                wildcard_plus = '/'.join(topic_parts[:-1]) + '/+'
                if wildcard_plus in self.upstream_topics:
                    print(f"上游代理已有通配符主题 {wildcard_plus}，不需要转发 {topic}")
                    return False

            # 检查 test/# 类型模式
            for i in range(len(topic_parts)):
                wildcard_hash = '/'.join(topic_parts[:i + 1]) + '/#'
                if wildcard_hash in self.upstream_topics:
                    print(f"上游代理已有通配符主题 {wildcard_hash}，不需要转发 {topic}")
                    return False

        print(f"上游代理没有相关主题，需要转发 {topic}")
        return True

    def forward_subscription_to_upstream(self, topic):
        """向上游代理转发订阅请求"""
        if self.upstream_broker is not None:
            print(f"代理 {self.id} 向上游代理 {self.upstream_broker} 转发订阅: {topic}")
            self.update_upstream_topics(topic)

    def unsubscription(self, topic):
        """处理取消订阅"""
        if topic in self.HT:
            self.HT.remove(topic)
        if topic in self.HS:
            self.HS.remove(topic)
        if topic in self.Tlb and self.id in self.Tlb[topic]:
            self.Tlb[topic].remove(self.id)
            if not self.Tlb[topic]:
                del self.Tlb[topic]

        print(f"代理 {self.id} 取消订阅主题: {topic}")

    def publication(self, topic, message, source_broker_id):
        """处理发布消息"""
        if source_broker_id == self.id:
            return False  # 不向自己转发

        # 检查是否有匹配的订阅
        for sub_topic in self.HT:
            if self.match_topic(topic, sub_topic):
                print(f"代理 {self.id} 接收来自 {source_broker_id} 的消息: {topic} - {message}")
                return True
        return False


# ARPS权重优选根代理管理器
class ARPSManager:
    def __init__(self):
        self.alpha = 0.21  # 连接度权重
        self.beta = 0.13  # 计算能力权重
        self.gamma = 0.19  # 延迟权重
        self.delta = 0.24  # 可靠性权重
        self.lambda_ = 0.23  # 功耗权重

        self.weight_history = []
        self.performance_history = []

    def calculate_node_scores(self, G, brokers):
        """计算节点评分（ARPS核心算法）"""
        scores = {}

        # 计算平均最短路径
        avg_shortest_paths = {}
        for broker in brokers:
            try:
                path_lengths = dict(nx.single_source_shortest_path_length(G, broker.id))
                avg_shortest_paths[broker.id] = np.mean(list(path_lengths.values())) if path_lengths else 0
            except:
                avg_shortest_paths[broker.id] = 0

        # 获取最大度和最大计算能力用于归一化
        degrees = [G.degree(broker.id) for broker in brokers if broker.id in G]
        max_degree = max(degrees) if degrees and max(degrees) > 0 else 1

        compute_powers = [broker.compute_power for broker in brokers]
        max_compute = max(compute_powers) if compute_powers else 1

        for broker in brokers:
            if broker.id not in G:
                continue

            d_v = G.degree(broker.id)  # 连接度
            C_v = broker.compute_power  # 计算能力
            P_v = broker.power_usage  # 功耗
            R_v = broker.reliability  # 可靠性
            D_v = avg_shortest_paths.get(broker.id, 0)  # 平均路径长度

            if D_v == 0:
                D_v = 0.001

            # ARPS评分计算
            score = (
                    self.alpha * (d_v / max_degree) +
                    self.beta * (C_v / max_compute) -
                    self.gamma * (1 / D_v) +
                    self.delta * R_v -
                    self.lambda_ * (P_v / 5.0)  # 归一化功耗
            )
            scores[broker.id] = score

        return scores

    def select_best_root(self, G, brokers):
        """选择最优根节点"""
        scores = self.calculate_node_scores(G, brokers)
        if not scores:
            return None, {}

        best_node_id = max(scores, key=scores.get)
        best_broker = next((b for b in brokers if b.id == best_node_id), brokers[0])

        print(f"ARPS选择的最佳根节点: {best_node_id}, 得分: {scores[best_node_id]:.4f}")
        print("各节点ARPS评分:", {k: f"{v:.4f}" for k, v in scores.items()})

        return best_broker, scores

    def adjust_weights(self, performance_metrics):
        """动态调整权重"""
        if len(self.performance_history) < 2:
            self.performance_history.append(performance_metrics)
            return

        prev_metrics = self.performance_history[-1]
        current_metrics = performance_metrics

        # 根据性能变化调整权重
        latency_change = current_metrics.get('avg_latency', 0) - prev_metrics.get('avg_latency', 0)
        delivery_change = current_metrics.get('delivery_rate', 0) - prev_metrics.get('delivery_rate', 0)

        if latency_change > 0.1:  # 延迟增加
            self.gamma *= 1.1  # 增加延迟权重
            self.alpha *= 0.9  # 减少连接度权重

        if delivery_change < -0.1:  # 传递效率下降
            self.delta *= 1.1  # 增加可靠性权重
            self.lambda_ *= 0.9  # 减少功耗权重

        # 归一化权重
        total = self.alpha + self.beta + self.gamma + self.delta + self.lambda_
        self.alpha /= total
        self.beta /= total
        self.gamma /= total
        self.delta /= total
        self.lambda_ /= total

        self.weight_history.append({
            'alpha': self.alpha,
            'beta': self.beta,
            'gamma': self.gamma,
            'delta': self.delta,
            'lambda_': self.lambda_
        })

        self.performance_history.append(current_metrics)


# 选择性发布订阅管理器
class SPFManager:
    def __init__(self):
        self.topic_cache = {}
        self.qos_levels = {}

    def _get_node_depth(self, broker_id, stp_tree, root_id, current_depth=0):
        """获取节点在树中的深度"""
        if broker_id == root_id:
            return current_depth

        for parent, children in stp_tree.items():
            if broker_id in children:
                return self._get_node_depth(parent, stp_tree, root_id, current_depth + 1)

        return -1  # 未找到

    def initialize_subscriptions(self, brokers, root_broker, stp_tree):
        """优化订阅初始化"""
        print("\n=== 初始化订阅 ===")
        for broker in brokers:
            if broker.id == root_broker.id:
                # 根代理订阅所有主题
                broker.subscription("#", {}, stp_tree)
                print(f"根代理 {broker.id} 订阅所有主题 (#)")
            else:
                # 根据代理位置和角色分配订阅
                depth = self._get_node_depth(broker.id, stp_tree, root_broker.id)
                if depth == 1:  # 直接子节点
                    # 订阅重要和传感器主题
                    broker.subscription("important/#", {}, stp_tree)
                    broker.subscription("sensor/#", {}, stp_tree)
                    broker.subscription("status/#", {}, stp_tree)
                    print(f"直接子节点 {broker.id} 订阅重要主题")
                else:  # 更深层的节点
                    # 只订阅关键主题和具体传感器数据
                    broker.subscription("critical/#", {}, stp_tree)
                    broker.subscription("sensor/data", {}, stp_tree)
                    broker.subscription("control/device", {}, stp_tree)
                    print(f"深层节点 {broker.id} 订阅关键主题")

    def _get_parent_id(self, broker_id, stp_tree):
        """获取代理在生成树中的父节点"""
        for parent, children in stp_tree.items():
            if broker_id in children:
                return parent
        return None

    def should_forward_message(self, source_broker, target_broker, topic, rtt_dict, stp_tree):
        """决定是否应该转发消息"""
        # 检查主题匹配
        for sub_topic in target_broker.HT:
            if target_broker.match_topic(topic, sub_topic):
                # 检查RTT阈值
                rtt = rtt_dict.get((source_broker.id, target_broker.id), float('inf'))
                if rtt > 100:  # RTT阈值
                    return False

                # 检查是否在有效的转发路径上
                if not self._is_valid_forwarding_path(source_broker.id, target_broker.id, stp_tree):
                    return False

                return True
        return False

    def _is_valid_forwarding_path(self, source_id, target_id, stp_tree):
        """检查是否是有效的转发路径"""
        # 在树状拓扑中，有效的转发路径是父子节点关系
        for parent, children in stp_tree.items():
            if source_id == parent and target_id in children:
                return True  # 父向子转发
            if target_id == parent and source_id in children:
                return True  # 子向父转发
        return False


# 主管理器：结合ARPS和SPF
class DWSManager:
    def __init__(self):
        self.arps_manager = ARPSManager()
        self.spf_manager = SPFManager()
        self.brokers = []
        self.root_broker = None
        self.rtt_dict = {}
        self.stp_tree = {}
        self.G = None

        # 负载均衡参数
        self.load_threshold = 0.7
        self.overload_count = 0
        self.max_overload_cycles = 3
        self.update_interval = 30
        self.last_tree_update = 0

        # 性能指标 - 修复统计问题
        self.metrics = {
            'total_messages': 0,  # 总发布消息数
            'successful_deliveries': 0,  # 成功投递次数
            'average_latency': [],
            'subscription_changes': 0,
            'tree_updates': 0,
            'root_changes': 0
        }

    async def setup_network(self, brokers):
        """设置网络拓扑"""
        try:
            self.brokers = brokers

            # 测量RTT
            self.rtt_dict = await self.measure_rtts()

            # 生成网络图
            self.G = self.generate_network_graph()

            # 使用ARPS选择根代理
            self.root_broker, scores = self.arps_manager.select_best_root(self.G, brokers)

            # 生成分层树结构
            self.stp_tree = self.generate_hierarchical_tree()

            # 设置上游代理关系
            self.setup_upstream_relationships()

            # 初始化SPF订阅
            self.spf_manager.initialize_subscriptions(brokers, self.root_broker, self.stp_tree)

            # 可视化网络
            self.visualize_network()

            print(f"\n网络设置完成。根代理: {self.root_broker.id}")
            print(f"树结构: {self.stp_tree}")

            # 显示初始订阅状态
            self.print_subscription_status()

        except Exception as e:
            print(f"网络设置错误: {e}")
            raise

    def print_subscription_status(self):
        """打印订阅状态"""
        print("\n=== 订阅状态 ===")
        for broker in self.brokers:
            print(f"代理 {broker.id}: {list(broker.HT)}")

    async def measure_rtts(self):
        """测量代理之间的RTT"""
        print("测量代理间RTT...")
        rtt_dict = {}
        for broker1 in self.brokers:
            for broker2 in self.brokers:
                if broker1.id != broker2.id:
                    rtt = await self.measure_single_rtt(broker1, broker2)
                    rtt_dict[(broker1.id, broker2.id)] = rtt
                    rtt_dict[(broker2.id, broker1.id)] = rtt
        return rtt_dict

    async def measure_single_rtt(self, broker1, broker2):
        """测量两个代理之间的RTT"""
        # 模拟RTT测量，基于端口差异产生不同延迟
        base_rtt = abs(broker1.port - broker2.port) * 5 + random.uniform(5, 20)
        return base_rtt

    def generate_network_graph(self):
        """生成网络图"""
        G = nx.Graph()

        # 添加节点
        for broker in self.brokers:
            G.add_node(broker.id,
                       compute_power=broker.compute_power,
                       reliability=broker.reliability,
                       power_usage=broker.power_usage)

        # 添加边和权重
        for (b1, b2), rtt in self.rtt_dict.items():
            if b1 < b2:  # 避免重复添加
                G.add_edge(b1, b2, weight=rtt)

        return G

    def generate_hierarchical_tree(self):
        """生成分层树结构"""
        if not self.G or not self.root_broker:
            return {}

        # 使用最短路径树
        try:
            tree = nx.shortest_path_tree(self.G, self.root_broker.id)
        except:
            # 如果图不连通，创建星型拓扑
            tree = {self.root_broker.id: []}
            for broker in self.brokers:
                if broker.id != self.root_broker.id:
                    tree[self.root_broker.id].append(broker.id)
            return tree

        # 转换为父节点映射
        parent_map = {}
        for node in tree.nodes():
            if node == self.root_broker.id:
                parent_map[node] = None
            else:
                path = list(nx.shortest_path(tree, self.root_broker.id, node))
                if len(path) >= 2:
                    parent_map[node] = path[-2]

        # 转换为子节点列表格式
        stp_tree = {broker.id: [] for broker in self.brokers}
        for node, parent in parent_map.items():
            if parent is not None:
                stp_tree[parent].append(node)

        return stp_tree

    def setup_upstream_relationships(self):
        """设置上游代理关系"""
        print("\n设置上游代理关系...")
        for broker in self.brokers:
            if broker.id != self.root_broker.id:
                parent_id = self._get_parent_id(broker.id, self.stp_tree)
                if parent_id:
                    broker.set_upstream_broker(parent_id)

    def _get_parent_id(self, broker_id, stp_tree):
        """获取代理的父节点ID"""
        for parent, children in stp_tree.items():
            if broker_id in children:
                return parent
        return None

    def handle_publication(self, source_broker, topic, message):
        """处理消息发布 - 修复统计逻辑"""
        self.metrics['total_messages'] += 1
        delivery_count = 0

        print(f"\n消息发布: {source_broker.id} -> {topic} - {message}")

        for broker in self.brokers:
            if broker.id != source_broker.id:
                if self.spf_manager.should_forward_message(source_broker, broker, topic,
                                                           self.rtt_dict, self.stp_tree):
                    if broker.publication(topic, message, source_broker.id):
                        delivery_count += 1
                        # 记录延迟
                        latency = self.rtt_dict.get((source_broker.id, broker.id), 0)
                        self.metrics['average_latency'].append(latency)

        # 修复：每个消息只增加一次成功投递计数
        if delivery_count > 0:
            self.metrics['successful_deliveries'] += 1

        print(f"消息投递结果: {topic} -> 成功投递到 {delivery_count} 个代理，当前投递率: {self.get_delivery_rate():.2f}")

    def get_delivery_rate(self):
        """计算投递率"""
        if self.metrics['total_messages'] == 0:
            return 0.0
        return self.metrics['successful_deliveries'] / self.metrics['total_messages']

    async def monitor_and_balance(self):
        """监控和负载均衡"""
        print("\n开始监控和负载均衡...")
        cycle_count = 0
        while True:
            try:
                cycle_count += 1
                print(f"\n=== 监控周期 {cycle_count} ===")

                # 显示当前负载状态
                print("当前负载状态:")
                for broker in self.brokers:
                    print(f"  代理 {broker.id}: 负载={broker.load:.2f}, 队列={broker.queue_length}")

                # 更新代理指标
                for broker in self.brokers:
                    self.update_broker_metrics(broker)

                # 收集性能指标
                performance_metrics = self.collect_performance_metrics()

                # ARPS动态调整权重
                self.arps_manager.adjust_weights(performance_metrics)

                # 检查根代理切换
                if await self.should_switch_root():
                    await self.update_tree_structure()

                # 更新订阅
                self.update_subscriptions()

                await asyncio.sleep(self.update_interval)

            except Exception as e:
                print(f"监控错误: {e}")
                await asyncio.sleep(5)

    def update_broker_metrics(self, broker):
        """更新代理性能指标"""
        # 模拟负载变化
        if broker.id == self.root_broker.id:
            # 根代理负载稍高
            load_change = random.uniform(-0.05, 0.15)
        else:
            load_change = random.uniform(-0.1, 0.1)

        broker.load = max(0.1, min(broker.load + load_change, 0.9))
        broker.queue_length = int(broker.load * 100)
        broker.power_usage = random.uniform(0.1, 5.0) * broker.load

    async def should_switch_root(self):
        """检查是否需要切换根代理"""
        current_load = self.root_broker.load

        if current_load > self.load_threshold:
            self.overload_count += 1
            print(f"根代理 {self.root_broker.id} 超载计数: {self.overload_count}/{self.max_overload_cycles}")

            if self.overload_count >= self.max_overload_cycles:
                # 重新计算节点得分
                scores = self.arps_manager.calculate_node_scores(self.G, self.brokers)
                best_candidate = max(self.brokers, key=lambda b: scores.get(b.id, 0))

                # 只有当候选节点明显优于当前根节点时才切换
                current_score = scores.get(self.root_broker.id, 0)
                candidate_score = scores.get(best_candidate.id, 0)

                if candidate_score > current_score * 1.1:
                    print(
                        f"根切换条件满足: {best_candidate.id} 得分 {candidate_score:.3f} > {self.root_broker.id} 得分 {current_score:.3f}")
                    return True
                else:
                    print(
                        f"候选节点提升不显著: {best_candidate.id} 得分 {candidate_score:.3f} vs 当前 {current_score:.3f}")
        else:
            if self.overload_count > 0:
                print(f"根代理 {self.root_broker.id} 负载恢复正常: {current_load:.2f}")
            self.overload_count = 0

        return False

    async def update_tree_structure(self):
        """更新树结构"""
        print("\n=== 更新树结构 ===")

        # 重新选择根代理
        new_root, scores = self.arps_manager.select_best_root(self.G, self.brokers)

        if new_root != self.root_broker:
            print(f"切换根代理: {self.root_broker.id} -> {new_root.id}")
            self.root_broker = new_root
            self.metrics['root_changes'] += 1
            self.overload_count = 0

        # 生成新的树结构
        new_tree = self.generate_hierarchical_tree()

        if new_tree != self.stp_tree:
            print("检测到树结构变化，重新配置...")
            self.stp_tree = new_tree
            self.setup_upstream_relationships()
            self.metrics['tree_updates'] += 1

            # 重新初始化订阅
            self.spf_manager.initialize_subscriptions(self.brokers, self.root_broker, self.stp_tree)
            self.metrics['subscription_changes'] += 1
        else:
            print("树结构未发生变化")

    def update_subscriptions(self):
        """根据负载调整订阅"""
        for broker in self.brokers:
            if broker.load > self.load_threshold and broker.id != self.root_broker.id:
                print(f"代理 {broker.id} 负载过高 ({broker.load:.2f})，调整订阅...")
                # 移除非关键主题订阅
                topics_to_remove = []
                for topic in broker.HT:
                    if not topic.startswith("critical/"):
                        topics_to_remove.append(topic)

                for topic in topics_to_remove:
                    broker.unsubscription(topic)
                    self.metrics['subscription_changes'] += 1

    def collect_performance_metrics(self):
        """收集性能指标"""
        metrics = {
            'avg_latency': np.mean(self.metrics['average_latency']) if self.metrics['average_latency'] else 0,
            'delivery_rate': self.get_delivery_rate(),
            'tree_height': self.calculate_tree_height(),
            'subscription_efficiency': len([b for b in self.brokers if b.HT]) / len(self.brokers)
        }
        return metrics

    def calculate_tree_height(self):
        """计算树高度"""

        def get_height(node, visited=None):
            if visited is None:
                visited = set()
            if node in visited:
                return 0
            visited.add(node)
            children = self.stp_tree.get(node, [])
            if not children:
                return 0
            return 1 + max(get_height(child, visited) for child in children)

        return get_height(self.root_broker.id) if self.root_broker else 0

    def visualize_network(self):
        """可视化网络结构"""
        if not self.G:
            return

        plt.figure(figsize=(12, 8))
        pos = nx.spring_layout(self.G, seed=42)

        # 绘制节点
        nx.draw_networkx_nodes(self.G, pos,
                               node_color=['red' if node == self.root_broker.id else 'lightblue'
                                           for node in self.G.nodes()],
                               node_size=500)

        # 绘制边
        nx.draw_networkx_edges(self.G, pos)

        # 添加标签
        labels = {node: f"B{node}" for node in self.G.nodes()}
        nx.draw_networkx_labels(self.G, pos, labels)

        plt.title("DWS MQTT Network with ARPS and SPF")
        plt.savefig('dws_network_topology.png')
        plt.close()
        print("网络拓扑图已保存为: dws_network_topology.png")

    async def start_brokers(self):
        """启动所有代理"""
        print("启动MQTT代理...")
        start_results = []
        for broker in self.brokers:
            result = await broker.start()
            start_results.append(result)
            if result:
                await asyncio.sleep(1)

        if all(start_results):
            print("所有代理启动成功")
            return True
        else:
            print("部分代理启动失败")
            return False

    async def stop_brokers(self):
        """停止所有代理"""
        print("\n停止所有代理...")
        for broker in self.brokers:
            await broker.stop()
        print("所有代理已停止")

    def get_performance_summary(self):
        """获取性能总结"""
        return {
            'message_delivery_rate': self.get_delivery_rate(),
            'average_latency': np.mean(self.metrics['average_latency']) if self.metrics['average_latency'] else 0,
            'subscription_changes': self.metrics['subscription_changes'],
            'tree_updates': self.metrics['tree_updates'],
            'root_changes': self.metrics['root_changes'],
            'total_messages': self.metrics['total_messages'],
            'successful_deliveries': self.metrics['successful_deliveries']
        }


# 主函数
async def main():
    # 创建代理节点
    brokers = [
        BrokerNode(
            id=1884,
            priority=1,
            address="localhost",
            port=1884,
            rtt=0,
            config_path="mosquitto_configs/broker1.conf"
        ),
        BrokerNode(
            id=1885,
            priority=2,
            address="localhost",
            port=1885,
            rtt=0,
            config_path="mosquitto_configs/broker2.conf"
        ),
        BrokerNode(
            id=1886,
            priority=3,
            address="localhost",
            port=1886,
            rtt=0,
            config_path="mosquitto_configs/broker3.conf"
        )
    ]

    # 创建DWS管理器
    manager = DWSManager()

    try:
        print("=== DWS MQTT 系统启动 ===")

        if not await manager.start_brokers():
            print("代理启动失败，系统退出")
            return

        await manager.setup_network(brokers)

        # 优化的测试场景
        test_scenarios = [
            {"topic": "important/sensor/temperature", "message": "25.5"},
            {"topic": "critical/alert/fire", "message": "emergency"},
            {"topic": "sensor/data/humidity", "message": "45%"},
            {"topic": "control/device/pump", "message": "start"},
            {"topic": "status/broker/health", "message": "ok"},
            {"topic": "sensor/data", "message": "raw_data"},
            {"topic": "important/notification", "message": "test"}
        ]

        # 启动监控
        monitor_task = asyncio.create_task(manager.monitor_and_balance())

        try:
            # 运行测试
            print("\n=== 开始消息发布测试 ===")
            for i, scenario in enumerate(test_scenarios):
                source_broker = brokers[i % len(brokers)]
                print(f"\n测试 {i + 1}/{len(test_scenarios)}:")
                manager.handle_publication(source_broker, scenario["topic"], scenario["message"])
                await asyncio.sleep(8)  # 等待消息传播

            # 运行一段时间观察负载均衡
            print("\n=== 进入负载均衡观察阶段 ===")
            await asyncio.sleep(60)

            # 打印性能总结
            print("\n=== 性能总结 ===")
            summary = manager.get_performance_summary()
            for key, value in summary.items():
                if isinstance(value, float):
                    print(f"{key}: {value:.4f}")
                else:
                    print(f"{key}: {value}")

        finally:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                print("监控任务已取消")

    except Exception as e:
        print(f"主函数错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await manager.stop_brokers()
        print("\n=== DWS MQTT 系统运行结束 ===")


if __name__ == "__main__":
    asyncio.run(main())
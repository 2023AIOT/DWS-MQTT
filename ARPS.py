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


# 移动进程管理类到前面
class ProcessManager:
    def __init__(self):
        self.processes = {}  # 进程字典
        self.ports = set()  # 使用的端口集合
        self.start_time = time.time()  # 启动时间

    def add_process(self, broker_id, process):
        """添加新的代理进程"""
        self.processes[broker_id] = process
        self.ports.add(broker_id)  # 端口通常是代理ID
        print(f"添加代理进程: {broker_id}, PID: {process.pid}")

    def check_port(self, port):
        """检查端口是否被占用"""
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        try:
            sock.bind(('127.0.0.1', port))
            sock.close()
            return False  # 端口未被占用
        except:
            sock.close()
            return True  # 端口被占用

    async def stop_all(self):
        """停止所有代理进程"""
        print("\n正在停止所有MQTT代理进程...")

        for broker_id, process in list(self.processes.items()):
            if process and process.poll() is None:
                try:
                    print(f"正在停止代理 {broker_id}...")
                    process.terminate()

                    # 等待进程终止(最多5秒)
                    for _ in range(5):
                        if process.poll() is not None:
                            print(f"代理 {broker_id} 已正常终止")
                            break
                        await asyncio.sleep(1)

                    # 如果仍在运行，强制终止
                    if process.poll() is None:
                        print(f"代理 {broker_id} 未响应，强制终止")
                        process.kill()
                except Exception as e:
                    print(f"停止代理 {broker_id} 时出错: {e}")

        # 清理所有可能残留的Mosquitto进程
        try:
            print("清理所有残留的Mosquitto进程...")
            subprocess.run(['taskkill', '/F', '/IM', 'mosquitto.exe'],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
        except Exception as e:
            print(f"清理Mosquitto进程时出错: {e}")

        # 确认端口已释放
        for port in self.ports:
            if self.check_port(port):
                print(f"警告: 端口 {port} 仍被占用")

        print("所有代理进程已停止")

    def get_running_count(self):
        """获取正在运行的进程数量"""
        return sum(1 for p in self.processes.values()
                   if p and p.poll() is None)

    def get_runtime(self):
        """返回运行时间(秒)"""
        return time.time() - self.start_time


# 移动自动停止控制器到前面
class AutoStopController:
    def __init__(self, max_runtime=300, max_root_changes=1, max_load_cycles=3):
        self.start_time = time.time()
        self.max_runtime = max_runtime  # 最大运行时间(秒)
        self.max_root_changes = max_root_changes  # 最大根节点变更次数
        self.max_load_cycles = max_load_cycles  # 最大负载周期数
        self.root_changes = 0  # 根节点变更计数
        self.load_cycles = 0  # 负载周期计数
        self.should_stop = False  # 是否应该停止
        self.stop_reason = ""  # 停止原因

    def check_stop_conditions(self, current_time=None):
        """检查是否应该停止"""
        if self.should_stop:
            return True

        if current_time is None:
            current_time = time.time()

        # 检查最大运行时间
        if current_time - self.start_time > self.max_runtime:
            self.should_stop = True
            self.stop_reason = f"达到最大运行时间: {self.max_runtime}秒"
            return True

        # 检查根节点变更次数
        if self.root_changes >= self.max_root_changes:
            self.should_stop = True
            self.stop_reason = f"根节点已变更 {self.root_changes} 次"
            return True

        # 检查负载周期
        if self.load_cycles >= self.max_load_cycles:
            self.should_stop = True
            self.stop_reason = f"完成 {self.load_cycles} 个负载测试周期"
            return True

        return False

    def record_root_change(self):
        """记录一次根节点变更"""
        self.root_changes += 1
        print(f"根节点变更次数: {self.root_changes}/{self.max_root_changes}")

    def record_load_cycle(self):
        """记录一次负载周期"""
        self.load_cycles += 1
        print(f"负载测试周期: {self.load_cycles}/{self.max_load_cycles}")

    def get_runtime(self):
        """获取当前运行时间"""
        return time.time() - self.start_time

    def force_stop(self, reason):
        """强制停止"""
        self.should_stop = True
        self.stop_reason = reason


# 代理结构
class BrokerNode(namedtuple('BrokerNode', ['id', 'priority', 'address', 'port', 'rtt', 'config_path'])):
    def __new__(cls, id, priority, address, port, rtt, config_path):
        self = super(BrokerNode, cls).__new__(cls, id, priority, address, port, rtt, config_path)
        self.load = 0  # 负载监控
        self.queue_length = 0  # 队列长度
        self.compute_power = np.random.randint(1, 100)  # 计算能力
        self.power_usage = np.random.uniform(0.1, 5.0)  # 功耗
        self.reliability = np.random.uniform(0.8, 1.0)  # 可靠性
        return self


# RTT 测量
async def measure_ping_rtt(address, broker):
    # 为每个端口定义固定的延迟时间
    port_delays = {
        1884: 10,  # 代理1: 10ms延迟
        1885: 20,  # 代理2: 20ms延迟
        1886: 30,  # 代理3: 30ms延迟
        1887: 15,  # 代理4: 15ms延迟
        1888: 25,  # 代理5: 25ms延迟
    }

    try:
        # 跳过Ping测量，直接使用TCP连接
        print(f"使用TCP连接测量RTT到代理 {broker.id}...")

        # 尝试建立TCP连接并测量时间
        start_time = time.time()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)

        try:
            sock.connect((address, broker.port))
            # 直接使用固定的延迟时间而不是实际测量的时间
            tcp_rtt = port_delays.get(broker.port, 10)
            sock.close()

            # 添加一些随机变动以模拟真实网络
            jitter = random.uniform(-2, 2)
            final_rtt = max(1, int(tcp_rtt + jitter))

            print(f"TCP连接RTT到代理 {broker.id}: {final_rtt} ms (含模拟延迟)")
            return broker.id, final_rtt
        except ConnectionRefusedError:
            # 如果端口未开放，使用模拟的RTT值
            simulated_rtt = port_delays.get(broker.port, 10)

            print(f"端口 {broker.port} 未开放，使用模拟的RTT值: {simulated_rtt} ms")
            return broker.id, simulated_rtt

    except Exception as e:
        print(f"测量代理 {broker.id} 的RTT时出错: {e}")
        return broker.id, None


# 测量所有代理的RTT
async def measure_broker_rtts(brokers, root_broker):
    try:
        tasks = []
        for broker in brokers:
            # 修改条件：当root_broker为None时测量所有broker，否则排除root_broker
            if root_broker is None or broker.id != root_broker.id:
                tasks.append(measure_ping_rtt(broker.address, broker))

        # 如果没有需要测量的broker，直接返回空字典
        if not tasks:
            print("警告: 没有可测量RTT的broker")
            return {}

        # 等待所有的 RTT 测量结果
        rtt_results = await asyncio.gather(*tasks)

        # 过滤出有效的RTT结果
        valid_results = {broker_id: rtt for broker_id, rtt in rtt_results if rtt is not None}

        # 如果没有有效的RTT测量结果，为所有broker设置默认值
        if not valid_results and brokers:
            print("警告: 所有RTT测量均失败，使用默认值")
            for broker in brokers:
                if root_broker and broker.id != root_broker.id:
                    valid_results[broker.id] = 10  # 默认RTT值为10ms

        return valid_results

    except Exception as e:
        print(f"Error in measure_broker_rtts: {e}")
        # 发生错误时，返回空字典而不是抛出异常
        return {}


# 计算邻接矩阵
def calculate_adjacency_matrix(brokers, rtt_dict):
    size = len(brokers)
    adj_matrix = [[float('inf')] * size for _ in range(size)]

    # 填充邻接矩阵
    for i, broker_i in enumerate(brokers):
        for j, broker_j in enumerate(brokers):
            if i == j:
                adj_matrix[i][j] = 0
            elif broker_j.id in rtt_dict:
                adj_matrix[i][j] = rtt_dict[broker_j.id]
    return adj_matrix


# Floyd-Warshall算法计算全局最短路径
def floyd_warshall(adj_matrix):
    size = len(adj_matrix)
    T = [[adj_matrix[i][j] for j in range(size)] for i in range(size)]  # 距离矩阵
    H = [[-1] * size for _ in range(size)]  # 下一跳矩阵

    # 初始化下一跳矩阵
    for i in range(size):
        for j in range(size):
            if adj_matrix[i][j] != float('inf'):
                H[i][j] = j

    # Floyd-Warshall核心算法
    for k in range(size):
        for i in range(size):
            for j in range(size):
                if T[i][k] != float('inf') and T[k][j] != float('inf'):
                    if T[i][j] > T[i][k] + T[k][j]:
                        T[i][j] = T[i][k] + T[k][j]
                        H[i][j] = H[i][k]
    return T, H


# 生成随机网络
def generate_random_graph(brokers, rtt_dict, edge_prob=0.3):
    """根据broker信息生成NetworkX图"""
    G = nx.Graph()

    # 添加所有节点
    for i, broker in enumerate(brokers):
        G.add_node(broker.id,
                   compute_power=broker.compute_power,
                   power_usage=broker.power_usage,
                   reliability=broker.reliability)

    # 添加边
    if rtt_dict:  # 如果有RTT测量结果，使用它们
        for i, broker_i in enumerate(brokers):
            for j, broker_j in enumerate(brokers):
                if i != j and broker_j.id in rtt_dict:
                    # 使用测量到的RTT作为边权重
                    G.add_edge(broker_i.id, broker_j.id, weight=rtt_dict[broker_j.id])
    else:  # 如果没有RTT测量结果，创建更有差异性的图
        print("警告: 没有有效的RTT测量结果，创建随机权重图")
        for i, broker_i in enumerate(brokers):
            for j, broker_j in enumerate(brokers):
                if i != j:
                    # 使用1-10之间的随机权重，增加路径差异性
                    if random.random() < edge_prob:  # 只有部分节点直接相连
                        weight = random.randint(1, 10)
                        G.add_edge(broker_i.id, broker_j.id, weight=weight)

    # 确保图是连通的
    if not nx.is_connected(G) and len(G.nodes()) > 1:
        print("警告: 图不是连通的，添加必要的边以确保连通性")
        components = list(nx.connected_components(G))
        for i in range(len(components) - 1):
            # 连接不同连通分量中的节点
            node1 = next(iter(components[i]))
            node2 = next(iter(components[i + 1]))
            G.add_edge(node1, node2, weight=random.randint(5, 15))  # 使用较大的随机权重

    # 计算并设置度数
    for node in G.nodes():
        G.nodes[node]["degree"] = G.degree(node)

    return G


# 计算最短路径平均距离
def calculate_average_shortest_path(G):
    avg_shortest_paths = {}
    for node in G.nodes():
        path_lengths = nx.shortest_path_length(G, source=node, weight='weight')
        avg_shortest_paths[node] = np.mean(list(path_lengths.values()))
    return avg_shortest_paths


# 计算评估分数 S(v)
def calculate_node_scores(G, alpha=0.21, beta=0.13, gamma=0.19, delta=0.24, lambda_=0.24):
    avg_shortest_paths = calculate_average_shortest_path(G)

    # 获取最大度和最大计算能力
    degrees = [G.degree(n) for n in G.nodes()]
    max_degree = max(degrees) if degrees and max(degrees) > 0 else 1  # 确保不为零

    compute_powers = [G.nodes[n]["compute_power"] for n in G.nodes()]
    max_compute = max(compute_powers) if compute_powers else 1  # 确保不为零

    scores = {}
    for node in G.nodes():
        d_v = G.nodes[node]["degree"]
        C_v = G.nodes[node]["compute_power"]
        P_v = G.nodes[node]["power_usage"]
        R_v = G.nodes[node]["reliability"]
        D_v = avg_shortest_paths[node]

        # 避免除以零
        if D_v == 0:
            D_v = 0.001  # 设置一个很小的非零值

        # 归一化计算得分
        S_v = (alpha * (d_v / max_degree) +
               beta * (C_v / max_compute) -
               gamma * (1 / D_v) +
               delta * R_v -
               lambda_ * P_v)

        scores[node] = S_v

    return scores


# 选择最优根节点
def select_best_root(G, brokers):
    scores = calculate_node_scores(G)
    best_node_id = max(scores, key=scores.get)

    # 找到对应的broker对象
    best_broker = next((b for b in brokers if b.id == best_node_id), None)

    print(f"根据NetworkX选择的最佳根节点: {best_node_id}")
    print("各节点评分:", scores)

    return best_broker, scores


# 可视化网络
def visualize_graph(G, best_node, stp_tree=None):
    pos = nx.spring_layout(G, seed=42)  # 使用固定seed确保布局一致性
    node_colors = ["red" if node == best_node else "skyblue" for node in G.nodes()]

    plt.figure(figsize=(12, 10))
    nx.draw(G, pos, with_labels=True, node_color=node_colors,
            node_size=800, edge_color="gray", width=2.0,
            font_size=12, font_weight='bold')

    # 添加边标签（RTT）
    edge_labels = nx.get_edge_attributes(G, "weight")
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=10)

    plt.title(f"MQTT Broker Network - Best Root Node: {best_node}", fontsize=16)
    plt.savefig("broker_network.png", dpi=300)  # 提高分辨率
    plt.close()  # 关闭当前图表

    # 只有当stp_tree存在时才创建树形图
    if stp_tree:
        # 创建并可视化生成树
        T = nx.Graph()
        for broker_id in G.nodes():
            T.add_node(broker_id)

        # 将连通拓扑添加到树
        for parent, children in stp_tree.items():
            for child in children:
                T.add_edge(parent, child)

        # 使用层次布局可视化树结构
        plt.figure(figsize=(12, 10))
        try:
            # 尝试使用graphviz布局，如果不可用则使用普通布局
            import pygraphviz
            pos_tree = nx.nx_agraph.graphviz_layout(T, prog="dot", root=best_node)
        except (ImportError, Exception):
            # 如果graphviz不可用，使用spring布局
            print("Graphviz不可用，使用spring布局代替")
            pos_tree = nx.spring_layout(T, seed=42)

        nx.draw(T, pos_tree, with_labels=True,
                node_color=["red" if node == best_node else "lightgreen" for node in T.nodes()],
                node_size=800, arrows=True, arrowsize=20,
                font_size=12, font_weight='bold')

        plt.title(f"MQTT Broker Spanning Tree - Root: {best_node}", fontsize=16)
        plt.savefig("broker_tree.png", dpi=300)
        plt.close()


# 选择最佳根代理（结合原有方法和NetworkX方法）
def elect_best_root_broker(brokers, T, G=None):
    if G is not None:
        # 使用NetworkX的方法选择
        return select_best_root(G, brokers)
    else:
        # 使用原来的方法选择
        best_broker = None
        min_max_distance = float('inf')
        candidates = []  # 存储候选根代理

        for i, broker in enumerate(brokers):
            max_distance = max(T[i])  # 计算Si
            if max_distance < min_max_distance:
                min_max_distance = max_distance
                best_broker = broker
                candidates = [broker]
            elif max_distance == min_max_distance:
                candidates.append(broker)

        # 如果有多个候选，选择优先级最高的
        if candidates:
            best_broker = min(candidates, key=lambda b: (b.priority, b.id))

        return best_broker, None


# 生成最短路径树
def generate_stp_tree(brokers, root_broker, H):
    size = len(brokers)
    root_index = next((i for i, b in enumerate(brokers) if b.id == root_broker.id), -1)
    stp = {broker.id: [] for broker in brokers}

    # 检查根节点是否有效
    if root_index == -1:
        print("错误: 无法找到根节点")
        return stp

    # 根据下一跳矩阵构建生成树
    for i, broker in enumerate(brokers):
        if i != root_index:
            next_hop = H[i][root_index]
            if next_hop != -1:
                parent_broker = brokers[next_hop]
                parent_id = parent_broker.id
                stp[parent_id].append(broker.id)
                print(f"节点 {broker.id} 的父节点为 {parent_id}")

    # 验证生成树
    print(f"生成树: {stp}")

    return stp


# 增强的负载均衡器
class LoadBalancer:
    def __init__(self, threshold=0.7, observation_cycles=3, cooldown_period=30):
        self.threshold = threshold  # 负载阈值
        self.lower_threshold = threshold - 0.1  # 下限阈值
        self.upper_threshold = threshold + 0.1  # 上限阈值
        self.candidates = []  # 候选根代理
        self.load_history = {}  # 负载历史记录
        self.observation_cycles = observation_cycles  # 需要观察的周期数
        self.high_load_count = 0  # 连续高负载计数
        self.last_switch_time = 0  # 上次切换时间
        self.cooldown_period = cooldown_period  # 冷却期(秒)
        self.load_distribution = {}  # 负载分布

    def update_broker_load(self, broker, cpu_load, queue_length):
        """更新代理负载信息"""
        old_load = broker.load
        broker.load = cpu_load
        broker.queue_length = queue_length

        if broker.id not in self.load_history:
            self.load_history[broker.id] = []
        self.load_history[broker.id].append((cpu_load, queue_length, time.time()))

        # 保持历史记录不超过20条
        if len(self.load_history[broker.id]) > 20:
            self.load_history[broker.id].pop(0)

        # 打印显著的负载变化
        if abs(old_load - cpu_load) > 0.1:
            print(f"代理 {broker.id} 负载变化显著: {old_load:.2f} -> {cpu_load:.2f}")

    def calculate_broker_score(self, broker):
        """计算代理的综合评分，考虑多种因素"""
        # 权重因子
        load_weight = 0.5  # CPU负载权重
        queue_weight = 0.2  # 队列长度权重
        stability_weight = 0.15  # 稳定性权重
        trend_weight = 0.15  # 趋势权重

        if broker.id in self.load_history and len(self.load_history[broker.id]) >= 2:
            # 提取历史数据
            history = self.load_history[broker.id]
            loads = [h[0] for h in history]
            queues = [h[1] for h in history]

            # 计算平均负载和队列长度
            avg_load = sum(loads) / len(loads)
            avg_queue = sum(queues) / len(queues)

            # 计算稳定性(方差的倒数)
            load_variance = np.var(loads) if len(loads) > 1 else 0
            stability = 1 / (1 + load_variance * 10)  # 越稳定，得分越低

            # 计算负载趋势(线性回归斜率)
            x = list(range(len(loads)))
            try:
                slope = np.polyfit(x, loads, 1)[0]
                # 正斜率表示负载增加趋势，增加评分
                trend = max(0, slope * 5)
            except:
                trend = 0

            # 计算总分(越低越好)
            return (load_weight * avg_load +
                    queue_weight * (avg_queue / 100) +
                    stability_weight * (1 - stability) +
                    trend_weight * trend)
        else:
            # 历史数据不足时简化计算
            return load_weight * broker.load + queue_weight * (broker.queue_length / 100)

    def calculate_load_distribution(self, brokers, stp_tree):
        """计算树结构中各节点负载分布"""
        distribution = {}

        # 检查stp_tree是否为None
        if stp_tree is None:
            print("警告: calculate_load_distribution中stp_tree为None，创建默认空树结构")
            stp_tree = {broker.id: [] for broker in brokers}

        # 递归计算子树负载
        def calculate_subtree_load(node_id):
            node_broker = next((b for b in brokers if b.id == node_id), None)
            if not node_broker:
                return 0

            children = stp_tree.get(node_id, [])
            children_load = sum(calculate_subtree_load(child) for child in children)

            # 存储此节点信息
            if node_id not in distribution:
                distribution[node_id] = {}

            distribution[node_id].update({
                'own_load': node_broker.load,
                'children_load': children_load,
                'total_load': node_broker.load + children_load,
                'children_count': len(children)
            })

            return node_broker.load + children_load

        # 从根节点开始计算
        for broker in brokers:
            calculate_subtree_load(broker.id)

        return distribution

    def update_candidates(self, brokers, current_root, stp_tree):
        """更新候选根代理列表，考虑多种因素"""
        self.candidates = []
        candidate_scores = []

        # 检查stp_tree是否为None
        if stp_tree is None:
            print("警告: update_candidates中stp_tree为None，创建默认空树结构")
            stp_tree = {broker.id: [] for broker in brokers}

        # 计算负载分布
        load_dist = self.calculate_load_distribution(brokers, stp_tree)
        self.load_distribution = load_dist

        for broker in brokers:
            if broker.id != current_root.id:
                # 基础负载分数
                score = self.calculate_broker_score(broker)

                # 修正因子: 子树负载过高的节点不适合成为根
                if broker.id in load_dist:
                    children_load = load_dist[broker.id].get('children_load', 0)
                    if children_load > 0.5:  # 子树负载超过50%
                        score += children_load * 0.3

                # 修正因子: 连接度过高或过低的节点不适合成为根
                children_count = len(stp_tree.get(broker.id, []))
                optimal_degree = (len(brokers) - 1) / 2  # 理想度数
                degree_penalty = abs(children_count - optimal_degree) * 0.02
                score += degree_penalty

                candidate_scores.append((score, broker))

        # 按分数排序(分数越低越好)
        sorted_candidates = sorted(candidate_scores, key=lambda x: x[0])
        # 最多保留5个候选节点
        top_candidates = sorted_candidates[:5]

        print("\n候选根节点排名:")
        for i, (score, broker) in enumerate(top_candidates):
            info = load_dist.get(broker.id, {})
            print(f"{i + 1}. 代理 {broker.id}: 分数={score:.4f}, 负载={broker.load:.2f}, "
                  f"子树负载={info.get('children_load', 0):.2f}, 子节点数={len(stp_tree.get(broker.id, []))}")

        # 将候选节点添加到最小堆
        for score, broker in top_candidates:
            heapq.heappush(self.candidates, (score, broker))

    def should_switch_root(self, current_root, brokers):
        """判断是否需要切换根代理，使用滞后策略"""
        current_time = time.time()

        # 如果在冷却期内，不允许切换
        if current_time - self.last_switch_time < self.cooldown_period:
            remaining = int(self.cooldown_period - (current_time - self.last_switch_time))
            print(f"根节点切换冷却中: 剩余{remaining}秒")
            return False

        # 计算当前根节点分数
        current_score = self.calculate_broker_score(current_root)

        # 检查是否在负载窗口内
        if self.lower_threshold <= current_root.load <= self.upper_threshold:
            self.high_load_count = 0  # 负载正常，重置计数
            return False

        # 检查高负载
        if current_root.load > self.upper_threshold:
            self.high_load_count += 1
            print(f"根节点 {current_root.id} 连续高负载: {self.high_load_count}/{self.observation_cycles}, "
                  f"当前负载: {current_root.load:.2f}")

            # 只有连续多个周期高负载才考虑切换
            if self.high_load_count >= self.observation_cycles:
                # 检查是否有明显更好的候选节点
                if self.candidates:
                    best_candidate_score, best_candidate = self.candidates[0]
                    # 只有当分数差异显著时才切换
                    if current_score - best_candidate_score > 0.2:
                        print(
                            f"找到更优根节点: {best_candidate.id}, 分数: {best_candidate_score:.4f} vs 当前: {current_score:.4f}")
                        return True
                    else:
                        print(
                            f"最佳候选节点 {best_candidate.id} 提升不显著: {best_candidate_score:.4f} vs 当前: {current_score:.4f}")
        else:
            # 负载正常，重置计数
            self.high_load_count = 0

        return False

    def get_next_root(self):
        """获取下一个根代理"""
        if self.candidates:
            self.last_switch_time = time.time()  # 记录切换时间
            return heapq.heappop(self.candidates)[1]
        return None

    async def monitor_and_balance(self, brokers, current_root, stp_tree):
        """监控和负载均衡主循环"""
        # 检查stp_tree是否为None，如果是则创建一个空的树结构
        if stp_tree is None:
            print("警告: stp_tree为None，创建默认空树结构")
            stp_tree = {broker.id: [] for broker in brokers}

        while True:
            try:
                print(f"\n当前根节点 {current_root.id} 负载: {current_root.load:.2f}")

                # 模拟负载变化 - 根节点负载稍高
                root_load_change = random.uniform(-0.05, 0.15)  # 根节点负载更容易增加
                new_root_load = max(0.3, min(current_root.load + root_load_change, 0.9))
                root_queue = int(new_root_load * 100)
                self.update_broker_load(current_root, new_root_load, root_queue)

                # 其他节点负载变化
                for broker in brokers:
                    if broker.id != current_root.id:
                        # 考虑层级关系的负载变化
                        is_child_of_root = broker.id in stp_tree.get(current_root.id, [])
                        # 根节点的直接子节点负载较高
                        if is_child_of_root:
                            load_change = random.uniform(-0.05, 0.1)
                        else:
                            load_change = random.uniform(-0.1, 0.05)

                        new_load = max(0.1, min(broker.load + load_change, 0.7))
                        queue_length = int(new_load * 100)
                        self.update_broker_load(broker, new_load, queue_length)

                # 更新候选根节点
                self.update_candidates(brokers, current_root, stp_tree)

                # 判断是否需要切换根节点
                if self.should_switch_root(current_root, brokers):
                    print(f"决定切换根节点: 当前节点 {current_root.id} 负载过高: {current_root.load:.2f}")
                    new_root = self.get_next_root()
                    if new_root:
                        print(
                            f"切换根节点: {current_root.id} ({current_root.load:.2f}) -> {new_root.id} ({new_root.load:.2f})")
                        return new_root

                await asyncio.sleep(5)

            except Exception as e:
                print(f"负载均衡监控错误: {e}")
                import traceback
                traceback.print_exc()
                await asyncio.sleep(5)


def detect_broker_changes(old_brokers, new_brokers):
    """检测代理变化"""
    old_broker_ids = {broker.id for broker in old_brokers}
    new_broker_ids = {broker.id for broker in new_brokers}

    added_brokers = [broker for broker in new_brokers if broker.id not in old_broker_ids]
    removed_brokers = [broker for broker in old_brokers if broker.id not in new_broker_ids]

    return added_brokers, removed_brokers


def generate_bridge_config(broker, root_broker):
    """生成代理的桥接配置"""
    config = {
        'port': broker.port,
        'allow_anonymous': True,
        'persistence': True,
        'log_type': 'all'
    }

    if broker.id != root_broker.id:
        config.update({
            'connection': f'bridge_{broker.id}_to_{root_broker.id}',
            'address': f'{root_broker.address}:{root_broker.port}',
            'topic': '#',
            'bridge_protocol_version': 'mqttv311',
            'cleansession': True,
            'try_private': True,
            'bridge_attempt_unsubscribe': False
        })

    return config


def write_config_to_file(config, config_path):
    """将配置写入文件"""
    try:
        with open(config_path, 'w') as f:
            f.write(f"port {config['port']}\n")
            f.write(f"allow_anonymous {str(config['allow_anonymous']).lower()}\n")
            f.write(f"persistence {str(config['persistence']).lower()}\n")
            f.write(f"log_type {config['log_type']}\n\n")

            if 'connection' in config:
                f.write(f"# Bridge configuration\n")
                f.write(f"connection {config['connection']}\n")
                f.write(f"address {config['address']}\n")
                f.write(f"topic {config['topic']} both 2\n")
                f.write(f"bridge_protocol_version {config['bridge_protocol_version']}\n")
                f.write(f"cleansession {str(config['cleansession']).lower()}\n")
                f.write(f"try_private {str(config['try_private']).lower()}\n")
                f.write(f"bridge_attempt_unsubscribe {str(config['bridge_attempt_unsubscribe']).lower()}\n")

        print(f"Configuration written to {config_path}")
    except Exception as e:
        print(f"Error writing configuration to {config_path}: {e}")
        raise


def start_broker(broker_id, config_path):
    """启动代理"""
    print(f"Starting Broker {broker_id} with config: {config_path}")
    try:
        process = subprocess.Popen(
            [r"C:\\Program Files\\mosquitto\\mosquitto.exe", "-c", config_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )

        if process.poll() is None:
            print(f"Broker {broker_id} started successfully with PID: {process.pid}")
        else:
            stdout, stderr = process.communicate()
            print(f"Broker {broker_id} failed to start:")
            print(f"stdout: {stdout}")
            print(f"stderr: {stderr}")

        return process
    except Exception as e:
        print(f"Error starting Broker {broker_id}: {e}")
        return None


# 收集性能指标
def collect_performance_metrics(brokers, root_broker, stp_tree, G):
    """收集性能指标和统计数据"""
    metrics = {
        "broker_count": len(brokers),
        "root_broker": root_broker.id,
        "timestamp": time.time()
    }

    # 计算树高度
    def calculate_tree_height(node, tree, visited=None):
        if visited is None:
            visited = set()

        if node in visited:
            return 0

        visited.add(node)

        if not tree[node]:
            return 0

        heights = [calculate_tree_height(child, tree, visited) for child in tree[node]]
        return 1 + max(heights) if heights else 0

    tree_height = calculate_tree_height(root_broker.id, stp_tree)
    metrics["tree_height"] = tree_height

    # 计算根节点度数
    metrics["root_degree"] = len(stp_tree[root_broker.id])

    # 计算平均节点度数
    total_degree = sum(len(children) for children in stp_tree.values())
    metrics["avg_node_degree"] = total_degree / len(brokers)

    # 使用NetworkX计算平均最短路径长度
    if nx.is_connected(G):
        avg_path_length = nx.average_shortest_path_length(G, weight='weight')
        metrics["avg_distance"] = avg_path_length

        # 计算最大距离
        max_distance = 0
        for u in G.nodes():
            for v in G.nodes():
                if u != v:
                    try:
                        path_length = nx.shortest_path_length(G, u, v, weight='weight')
                        max_distance = max(max_distance, path_length)
                    except nx.NetworkXNoPath:
                        pass
        metrics["max_distance"] = max_distance
    else:
        metrics["avg_distance"] = float('inf')
        metrics["max_distance"] = float('inf')
        print("警告: 图不是连通的，无法计算平均最短路径长度")

    # 打印指标
    print("\n性能指标:")
    for key, value in metrics.items():
        print(f"{key}: {value}")

    return metrics


# 模拟代理负载变化
def simulate_broker_load(brokers, load_balancer, simulation_time=60):
    """模拟代理负载变化以测试负载均衡机制"""
    print("\n开始负载模拟测试...")

    start_time = time.time()
    load_changes = []

    # 模拟突发流量
    for cycle in range(3):  # 模拟3个负载周期
        # 选择一个代理承受高负载
        target_broker = brokers[np.random.randint(0, len(brokers))]
        print(f"模拟周期 {cycle + 1}: 代理 {target_broker.id} 将承受高负载")

        cycle_start = time.time()
        while time.time() - cycle_start < simulation_time / 3:
            # 主要增加目标代理的负载
            load_increase = np.random.uniform(0.1, 0.2)
            new_load = min(target_broker.load + load_increase, 1.0)

            # 更新代理负载
            old_load = target_broker.load
            queue_length = int(new_load * 100)
            load_balancer.update_broker_load(target_broker, new_load, queue_length)

            # 记录负载变化
            load_changes.append({
                "timestamp": time.time() - start_time,
                "broker_id": target_broker.id,
                "old_load": old_load,
                "new_load": new_load,
                "queue_length": queue_length
            })

            print(
                f"时间 {time.time() - start_time:.1f}s: 代理 {target_broker.id} 负载从 {old_load:.2f} 增加到 {new_load:.2f}")

            # 其他代理负载变化较小
            for broker in brokers:
                if broker.id != target_broker.id:
                    load_change = np.random.uniform(-0.05, 0.05)
                    new_load = max(0, min(broker.load + load_change, 0.5))
                    queue_length = int(new_load * 100)
                    load_balancer.update_broker_load(broker, new_load, queue_length)

            time.sleep(np.random.uniform(0.5, 2.0))

    print("\n负载模拟测试完成")
    return load_changes


# 改进的树结构生成
def generate_hierarchical_tree(brokers, root_broker, G):
    """生成多层级树结构，避免所有节点都连接到根节点"""
    # 初始化生成树结构
    stp = {broker.id: [] for broker in brokers}

    # 计算每个节点的重要性指标
    centrality_dict = {}
    try:
        # 尝试计算中心性指标
        betweenness = nx.betweenness_centrality(G)
        closeness = nx.closeness_centrality(G)

        for node in G.nodes():
            # 综合得分 = 0.5*介数中心性 + 0.5*接近中心性
            centrality_dict[node] = 0.5 * betweenness.get(node, 0) + 0.5 * closeness.get(node, 0)
    except Exception as e:
        print(f"计算中心性指标失败: {e}")
        # 使用度数作为备选指标
        for node in G.nodes():
            centrality_dict[node] = G.degree(node)

    # 按中心性排序节点（除了根节点）
    non_root_nodes = [b.id for b in brokers if b.id != root_broker.id]
    sorted_nodes = sorted(non_root_nodes,
                          key=lambda node: centrality_dict.get(node, 0),
                          reverse=True)

    # 确定层数
    node_count = len(brokers)
    if node_count <= 3:
        # 小型网络使用单层结构
        print("节点数量较少，使用单层树结构")
        for node in sorted_nodes:
            stp[root_broker.id].append(node)
    else:
        # 大型网络使用多层结构
        print("节点数量较多，构建多层树结构")

        # 选择前20%的节点作为中继节点(至少1个，最多n-2个)
        relay_count = max(1, min(int(node_count * 0.2), node_count - 2))
        relay_nodes = sorted_nodes[:relay_count]
        leaf_nodes = sorted_nodes[relay_count:]

        print(f"选择 {relay_count} 个中继节点: {relay_nodes}")

        # 连接根节点到中继节点
        for relay in relay_nodes:
            stp[root_broker.id].append(relay)

        # 分配叶节点到最近的中继节点
        for leaf in leaf_nodes:
            best_relay = None
            best_distance = float('inf')

            # 找到最近的中继节点
            for relay in relay_nodes:
                try:
                    distance = nx.shortest_path_length(G, source=leaf, target=relay, weight='weight')
                    if distance < best_distance:
                        best_distance = distance
                        best_relay = relay
                except:
                    continue

            # 如果找到合适的中继节点，连接到它
            if best_relay:
                stp[best_relay].append(leaf)
                print(f"节点 {leaf} 连接到中继节点 {best_relay} (距离: {best_distance})")
            else:
                # 无法找到合适的中继节点，直接连接到根
                stp[root_broker.id].append(leaf)
                print(f"节点 {leaf} 无法找到合适的中继节点，直接连接到根节点")

    # 确保所有节点都被分配
    assigned_nodes = set()
    for parent, children in stp.items():
        assigned_nodes.add(parent)
        for child in children:
            assigned_nodes.add(child)

    # 检查是否有未分配节点
    all_nodes = {broker.id for broker in brokers}
    unassigned = all_nodes - assigned_nodes
    if unassigned:
        print(f"发现未分配节点: {unassigned}")
        for node in unassigned:
            stp[root_broker.id].append(node)
            print(f"未分配节点 {node} 直接连接到根节点")

    # 打印树结构
    print("\n生成的树结构:")
    for parent, children in stp.items():
        if children:
            print(f"节点 {parent} 的子节点: {children}")

    # 计算并打印树的高度
    def calculate_height(node, visited=None):
        if visited is None:
            visited = set()
        if node in visited:
            return 0
        visited.add(node)
        if not stp[node]:
            return 0
        return 1 + max(calculate_height(child, visited) for child in stp[node])

    tree_height = calculate_height(root_broker.id)
    print(f"生成树高度: {tree_height}")

    return stp


async def update_broker_topology(old_brokers, new_brokers, root_broker):
    """更新代理拓扑"""
    try:
        added_brokers, removed_brokers = detect_broker_changes(old_brokers, new_brokers)

        if added_brokers or removed_brokers:
            print("Broker topology changed. Recomputing routes.")
            rtt_results = await measure_broker_rtts(new_brokers, root_broker)
            rtt_dict = {broker_id: rtt for broker_id, rtt in rtt_results.items() if rtt is not None}

            # 创建NetworkX图
            G = generate_random_graph(new_brokers, rtt_dict)

            # 可视化网络
            visualize_graph(G, root_broker.id)

            adj_matrix = calculate_adjacency_matrix(new_brokers, rtt_dict)
            T, H = floyd_warshall(adj_matrix)
            stp_tree = generate_hierarchical_tree(new_brokers, root_broker, G)

            # 收集并打印性能指标
            collect_performance_metrics(new_brokers, root_broker, stp_tree, G)

            processes = []
            for broker in new_brokers:
                try:
                    config = generate_bridge_config(broker, root_broker)
                    write_config_to_file(config, broker.config_path)

                    process = start_broker(broker.id, broker.config_path)
                    if process:
                        processes.append(process)

                    await asyncio.sleep(2)

                except Exception as e:
                    print(f"Error configuring Broker {broker.id}: {e}")

            print("All brokers started. Waiting for stability...")
            await asyncio.sleep(5)

            for process in processes:
                if process.poll() is None:
                    print(f"Broker process {process.pid} is running")
                else:
                    print(f"Broker process {process.pid} has terminated")

            return G, stp_tree
    except Exception as e:
        print(f"Error in updating broker topology: {e}")

    return None, None


async def setup_brokers():
    try:
        # 这里暂时不创建类实例
        global process_manager

        # 初始化代理节点
        brokers = [
            BrokerNode(id=1884, priority=1, address="localhost", port=1884, rtt=0,
                       config_path=r"D:\mosquitto-configs\mosquitto_broker1.conf"),
            BrokerNode(id=1885, priority=2, address="localhost", port=1885, rtt=0,
                       config_path=r"D:\mosquitto-configs\mosquitto_broker2.conf"),
            BrokerNode(id=1886, priority=3, address="localhost", port=1886, rtt=0,
                       config_path=r"D:\mosquitto-configs\mosquitto_broker3.conf"),
            BrokerNode(id=1887, priority=4, address="localhost", port=1887, rtt=0,
                       config_path=r"D:\mosquitto-configs\mosquitto_broker4.conf"),
            BrokerNode(id=1888, priority=5, address="localhost", port=1888, rtt=0,
                       config_path=r"D:\mosquitto-configs\mosquitto_broker5.conf"),
        ]

        # 先启动MQTT代理
        print("\n开始启动MQTT代理...")
        processes = []
        for broker in brokers:
            try:
                config = {
                    'port': broker.port,
                    'allow_anonymous': True,
                    'persistence': True,
                    'log_type': 'all'
                }
                write_config_to_file(config, broker.config_path)
                process = start_broker(broker.id, broker.config_path)
                if process:
                    processes.append(process)
                await asyncio.sleep(2)
            except Exception as e:
                print(f"启动代理 {broker.id} 时出错: {e}")

        await asyncio.sleep(5)  # 等待所有代理启动

        # 再设置网络延迟
        print("\n开始设置网络延迟以模拟真实环境...")
        # 这里暂时不添加网络延迟设置

        # 等待延迟生效
        await asyncio.sleep(3)

        # 创建负载均衡器
        load_balancer = LoadBalancer(
            threshold=0.7,  # 负载阈值
            observation_cycles=2,  # 2个周期高负载触发切换
            cooldown_period=30  # 30秒冷却期
        )

        # 测量RTT并构建网络图
        print("\n开始RTT测量...")
        rtt_results = await measure_broker_rtts(brokers, None)
        rtt_dict = {broker_id: rtt for broker_id, rtt in rtt_results.items() if rtt is not None}

        # 使用NetworkX构建图并选择最佳根节点
        G = generate_random_graph(brokers, rtt_dict)
        root_broker, scores = elect_best_root_broker(brokers, None, G)

        # 如果NetworkX方法没有选出根节点，则使用原始方法
        if root_broker is None:
            adj_matrix = calculate_adjacency_matrix(brokers, rtt_dict)
            T, H = floyd_warshall(adj_matrix)
            root_broker, _ = elect_best_root_broker(brokers, T)

        print(f"Selected root broker: {root_broker.id}")

        # 可视化网络
        visualize_graph(G, root_broker.id)

        # 生成生成树
        adj_matrix = calculate_adjacency_matrix(brokers, rtt_dict)
        T, H = floyd_warshall(adj_matrix)
        stp_tree = generate_hierarchical_tree(brokers, root_broker, G)

        # 可视化完整树形结构
        visualize_graph(G, root_broker.id, stp_tree)

        # 收集初始性能指标
        initial_metrics = collect_performance_metrics(brokers, root_broker, stp_tree, G)

        G, stp_tree = await update_broker_topology([], brokers, root_broker)

        # 如果stp_tree为None，创建默认树结构
        if stp_tree is None:
            print("警告: update_broker_topology返回的stp_tree为None，使用原始树结构")
            # 使用之前生成的树结构
            stp_tree = generate_hierarchical_tree(brokers, root_broker, G)

        # 运行负载模拟测试
        load_changes = simulate_broker_load(brokers, load_balancer, simulation_time=30)

        # 记录负载变化后的指标
        if G and stp_tree:
            after_load_metrics = collect_performance_metrics(brokers, root_broker, stp_tree, G)

            print("\n负载变化前后性能对比:")
            for key in initial_metrics:
                if key in after_load_metrics:
                    print(f"{key}: {initial_metrics[key]} -> {after_load_metrics[key]}")

        root_changes = []
        start_monitor_time = time.time()

        monitor_duration = 60  # 监控1分钟
        print(f"\n开始负载均衡监控 ({monitor_duration} 秒)...")

        while time.time() - start_monitor_time < monitor_duration:
            new_root = await load_balancer.monitor_and_balance(brokers, root_broker, stp_tree)
            if new_root and new_root != root_broker:
                print(f"\n根节点切换: {root_broker.id} -> {new_root.id}")
                old_root = root_broker
                root_broker = new_root

                # 记录根节点变化
                root_changes.append({
                    "timestamp": time.time() - start_monitor_time,
                    "old_root": old_root.id,
                    "new_root": new_root.id,
                    "old_root_load": old_root.load,
                    "new_root_load": new_root.load
                })

                G, stp_tree = await update_broker_topology(brokers, brokers, root_broker)

                if G and stp_tree:
                    # 收集根节点变化后的指标
                    new_metrics = collect_performance_metrics(brokers, root_broker, stp_tree, G)

                    print("\n根节点变化后性能对比:")
                    for key in initial_metrics:
                        if key in new_metrics:
                            print(f"{key}: {initial_metrics[key]} -> {new_metrics[key]}")

            await asyncio.sleep(5)

        print("\n监控完成！")
        print(f"根节点变化次数: {len(root_changes)}")
        for change in root_changes:
            print(
                f"时间 {change['timestamp']:.1f}s: {change['old_root']} -> {change['new_root']} (负载: {change['old_root_load']:.2f} -> {change['new_root_load']:.2f})")

    except Exception as e:
        print(f"Error in setup_brokers: {e}")
        raise
    finally:
        # 清理网络延迟设置
        print("清理网络延迟设置...")
        # 这里暂时不清理网络延迟设置


def main():
    """主函数"""
    try:
        # 在这里创建类实例
        global process_manager
        process_manager = ProcessManager()

        print("Starting the setup process!")
        asyncio.run(setup_brokers())
        print("Setup completed successfully!")
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error in main process: {e}")
        raise
    finally:
        # 清理网络延迟设置
        print("清理网络延迟设置...")
        # 这里暂时不清理网络延迟设置


# 确保类定义在调用之前完成
if __name__ == "__main__":
    # 类定义在这之前
    main()
import time
import psutil
import gc
import threading
import contextlib
from typing import Optional, List, Generator, Any
from dataclasses import dataclass

@dataclass
class TimingResult:
    start_time: float
    end_time: float = 0
    
    @property
    def duration(self) -> float:
        """返回持续时间(秒)"""
        return self.end_time - self.start_time

@dataclass
class MemoryResult:
    start_memory: float
    peak_memory: float = 0
    end_memory: float = 0
    samples: List[float] = None
    sampling_interval: float = 0.1  # 采样间隔(秒)
    
    def __post_init__(self):
        self.samples = []
    
    @property
    def peak_mb(self) -> float:
        """返回峰值内存(MB)"""
        return self.peak_memory / (1024 * 1024)
    
    @property
    def used_mb(self) -> float:
        """返回使用的内存(MB)"""
        return (self.end_memory - self.start_memory) / (1024 * 1024)
    
    @property
    def average_mb(self) -> float:
        """返回平均内存使用(MB)"""
        if not self.samples:
            return 0
        return sum(self.samples) / (len(self.samples) * 1024 * 1024)

class MemoryMonitor:
    """内存使用监控器"""
    
    def __init__(self, result: MemoryResult):
        self.result = result
        self.process = psutil.Process()
        self.running = False
        self.monitor_thread = None
    
    def _monitor(self):
        while self.running:
            try:
                current_memory = self.process.memory_info().rss
                self.result.samples.append(current_memory)
                self.result.peak_memory = max(
                    self.result.peak_memory, 
                    current_memory
                )
                time.sleep(self.result.sampling_interval)
            except Exception:
                break
    
    def start(self):
        """开始监控"""
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
    
    def stop(self):
        """停止监控"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1.0)

@contextlib.contextmanager
def measure_time() -> Generator[TimingResult, None, None]:
    """
    测量代码块执行时间的上下文管理器
    
    使用示例:
    with measure_time() as timer:
        # 执行需要测量的代码
        ...
    print(f"执行时间: {timer.duration:.2f}秒")
    """
    # 在测量前先让系统状态稳定
    time.sleep(0.1)
    
    result = TimingResult(start_time=time.time())
    try:
        yield result
    finally:
        # 确保获取准确的结束时间
        result.end_time = time.time()

@contextlib.contextmanager
def measure_memory() -> Generator[MemoryResult, None, None]:
    """
    测量代码块内存使用的上下文管理器
    
    使用示例:
    with measure_memory() as mem:
        # 执行需要监控的代码
        ...
    print(f"峰值内存: {mem.peak_mb:.2f}MB")
    print(f"平均内存: {mem.average_mb:.2f}MB")
    """
    # 在测量前先让系统状态稳定
    time.sleep(0.1)
    
    # 初始化内存监控
    process = psutil.Process()
    initial_memory = process.memory_info().rss
    
    # 创建结果对象并启动监控
    result = MemoryResult(start_memory=initial_memory)
    monitor = MemoryMonitor(result)
    
    try:
        # 启动监控线程
        monitor.start()
        # 记录实际开始时的内存
        result.start_memory = process.memory_info().rss
        result.peak_memory = result.start_memory
        
        yield result
        
    finally:
        try:
            # 获取最终内存状态
            final_memory = process.memory_info().rss
            
            # 停止监控
            monitor.stop()
            
            # 更新结果
            result.end_memory = final_memory
            if result.samples:
                # 使用所有采样点计算峰值
                result.peak_memory = max(
                    result.peak_memory,
                    final_memory,
                    max(result.samples)
                )
            else:
                # 如果没有采样点，使用开始和结束值
                result.peak_memory = max(
                    result.start_memory,
                    result.end_memory
                )
                
        except Exception as e:
            print(f"警告: 内存监控结束时发生错误: {e}")
            # 确保至少有基本的内存使用数据
            if not result.end_memory:
                result.end_memory = result.start_memory
            if not result.peak_memory:
                result.peak_memory = max(
                    result.start_memory,
                    result.end_memory
                )

def calculate_throughput(operation_count: int, duration: float) -> float:
    """
    计算操作吞吐量
    
    Args:
        operation_count: 操作次数
        duration: 持续时间(秒)
    
    Returns:
        每秒操作次数
    """
    return operation_count / duration if duration > 0 else 0

def get_memory_stats() -> dict:
    """获取当前内存使用统计"""
    process = psutil.Process()
    mem_info = process.memory_info()
    
    return {
        'rss': mem_info.rss / (1024 * 1024),  # RSS内存(MB)
        'vms': mem_info.vms / (1024 * 1024),  # 虚拟内存(MB)
        'shared': getattr(mem_info, 'shared', 0) / (1024 * 1024),  # 共享内存(MB)
        'percent': process.memory_percent(),  # 内存使用百分比
    } 
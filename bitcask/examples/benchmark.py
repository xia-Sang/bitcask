import os
import time
import json
import random
import string
import tempfile
from typing import Dict, List, Any

from bitcask.bitcask import Bitcask
from bitcask.config import ConfigManager

def generate_random_string(length: int) -> str:
    """生成指定长度的随机字符串"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_test_data(num_records: int, key_length: int = 10, value_size: int = 100) -> Dict[str, Dict[str, Any]]:
    """生成测试数据"""
    data = {}
    for i in range(num_records):
        key = f"key_{generate_random_string(key_length)}"
        value = {
            "id": i,
            "timestamp": time.time(),
            "data": generate_random_string(value_size)
        }
        data[key] = value
    return data

def run_write_benchmark(bitcask: Bitcask, data: Dict[str, Dict[str, Any]]) -> float:
    """运行写入性能测试"""
    start_time = time.time()
    
    for key, value in data.items():
        bitcask.put(key, value)
    
    end_time = time.time()
    return end_time - start_time

def run_read_benchmark(bitcask: Bitcask, keys: List[str], num_reads: int) -> float:
    """运行读取性能测试"""
    start_time = time.time()
    
    for _ in range(num_reads):
        key = random.choice(keys)
        value = bitcask.get(key)
        assert value is not None
    
    end_time = time.time()
    return end_time - start_time

def run_scan_benchmark(bitcask: Bitcask, prefix: str) -> float:
    """运行扫描性能测试"""
    start_time = time.time()
    
    items = list(bitcask.scan_prefix(prefix))
    
    end_time = time.time()
    return end_time - start_time, len(items)

def verify_data_consistency(bitcask: Bitcask, original_data: Dict[str, Dict[str, Any]]) -> tuple[bool, list]:
    """验证写入的数据和读取的数据是否一致"""
    inconsistent_keys = []
    for key, expected_value in original_data.items():
        actual_value = bitcask.get(key)
        if actual_value != expected_value:
            inconsistent_keys.append((key, expected_value, actual_value))
    
    return len(inconsistent_keys) == 0, inconsistent_keys

def main():
    # 创建临时目录
    test_dir = tempfile.mkdtemp()
    try:
        # 创建配置
        config = {
            "storage": {
                "data_dir": os.path.join(test_dir, "data"),
                "max_file_size": 67108864,  # 64MB
                "enable_compression": False,
                "compression_threshold": 1024
            },
            "wal": {
                "wal_dir": os.path.join(test_dir, "wal"),
                "max_wal_size": 67108864,  # 64MB
                "sync_strategy": "always",
                "sync_interval": 1.0,
                "retention_hours": 24,
                "auto_recovery": True
            },
            "btree": {
                "enable_cache": True,
                "max_cache_size": 1000000,
                "cache_eviction": "lru"
            },
            "transaction": {
                "timeout": 30,
                "max_retries": 3,
                "isolation_level": "serializable"
            },
            "performance": {
                "compaction_interval": 3600,
                "read_buffer_size": 65536,
                "write_buffer_size": 65536,
                "enable_prefetch": True,
                "prefetch_size": 1000
            },
            "monitoring": {
                "log_level": "info",
                "enable_metrics": True,
                "metrics_interval": 60,
                "slow_query_log": True,
                "slow_query_threshold": 100
            }
        }
        config_path = os.path.join(test_dir, "config.json")
        with open(config_path, "w") as f:
            json.dump(config, f, indent=4)
        
        # 初始化 Bitcask
        bitcask = Bitcask(config_path)
        
        # 生成测试数据
        num_records = 10000
        print(f"\n生成 {num_records} 条测试数据...")
        test_data = generate_test_data(num_records)
        
        # 写入性能测试
        print("\n开始写入性能测试...")
        write_time = run_write_benchmark(bitcask, test_data)
        print(f"写入 {num_records} 条记录耗时: {write_time:.2f} 秒")
        print(f"写入速度: {num_records / write_time:.2f} ops/s")
        
        # 数据一致性验证
        print("\n开始数据一致性验证...")
        is_consistent, inconsistent_keys = verify_data_consistency(bitcask, test_data)
        if is_consistent:
            print("数据一致性验证通过！所有数据都正确写入和读取。")
        else:
            print(f"数据一致性验证失败！发现 {len(inconsistent_keys)} 个不一致的键值对：")
            for key, expected, actual in inconsistent_keys[:5]:  # 只显示前5个不一致的数据
                print(f"\nKey: {key}")
                print(f"预期值: {expected}")
                print(f"实际值: {actual}")
            if len(inconsistent_keys) > 5:
                print(f"\n... 还有 {len(inconsistent_keys) - 5} 个不一致的键值对未显示")
        
        # 读取性能测试
        num_reads = 10000
        keys = list(test_data.keys())
        print(f"\n开始读取性能测试 ({num_reads} 次随机读取)...")
        read_time = run_read_benchmark(bitcask, keys, num_reads)
        print(f"随机读取 {num_reads} 次耗时: {read_time:.2f} 秒")
        print(f"读取速度: {num_reads / read_time:.2f} ops/s")
        
        # 扫描性能测试
        prefix = "key_"
        print("\n开始扫描性能测试...")
        scan_time, num_items = run_scan_benchmark(bitcask, prefix)
        print(f"扫描 {num_items} 条记录耗时: {scan_time:.2f} 秒")
        print(f"扫描速度: {num_items / scan_time:.2f} items/s")
        
        # 关闭实例
        bitcask.close()
        
    finally:
        # 清理临时目录
        import shutil
        try:
            shutil.rmtree(test_dir)
        except Exception as e:
            print(f"清理临时目录失败: {e}")

if __name__ == "__main__":
    main()

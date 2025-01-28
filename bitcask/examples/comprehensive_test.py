import os
import json
import time
import random
import tempfile
import pytest
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor
from bitcask.bitcask import Bitcask
from bitcask.utils import (
    generate_random_data,
    generate_key_series,
    generate_large_value,
    generate_sequential_data,
    generate_test_dataset
)
from bitcask.utils.performance import (
    measure_time,
    measure_memory,
    calculate_throughput
)
from bitcask.utils.validation import (
    verify_data_consistency,
    compare_data_sets
)

@pytest.fixture
def test_config(tmp_path):
    """创建测试配置"""
    config = {
        "storage": {
            "data_dir": str(tmp_path / "data"),
            "max_file_size": 67108864,
            "enable_compression": False,
            "compression_threshold": 1024
        },
        "wal": {
            "wal_dir": str(tmp_path / "wal"),
            "max_wal_size": 67108864,
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
    
    config_path = tmp_path / "config.json"
    with open(config_path, "w") as f:
        json.dump(config, f, indent=4)
    
    return str(config_path)

@pytest.fixture
def bitcask(test_config):
    """创建 Bitcask 实例"""
    db = Bitcask(test_config)
    yield db
    db.close()

def test_basic_operations(bitcask: Bitcask):
    """测试基本的读写操作"""
    print("\n=== 测试基本读写操作 ===")
    
    # 使用数据生成器创建测试数据
    test_data = generate_random_data(record_count=1000)
    
    # 批量写入测试
    print("\n1. 批量写入测试:")
    with measure_time() as write_time:
        with measure_memory() as write_mem:
            for key, value in test_data.items():
                bitcask.put(key, value)
    
    throughput = calculate_throughput(len(test_data), write_time.duration)
    print(f"批量写入耗时: {write_time.duration:.2f}秒")
    print(f"写入吞吐量: {throughput:.2f} ops/s")
    print(f"内存使用: {write_mem.peak_mb:.2f}MB")
    
    # 批量读取测试
    print("\n2. 批量读取测试:")
    with measure_time() as read_time:
        with measure_memory() as read_mem:
            read_data = {key: bitcask.get(key) for key in test_data.keys()}
    
    throughput = calculate_throughput(len(test_data), read_time.duration)
    print(f"批量读取耗时: {read_time.duration:.2f}秒")
    print(f"读取吞吐量: {throughput:.2f} ops/s")
    print(f"内存使用: {read_mem.peak_mb:.2f}MB")
    
    # 数据一致性验证
    missing, extra, different = compare_data_sets(test_data, read_data)
    print("\n3. 数据一致性验证:")
    print(f"丢失的键: {len(missing)}")
    print(f"多余的键: {len(extra)}")
    print(f"不一致的值: {len(different)}")
    if not any([missing, extra, different]):
        print("数据完全一致 ✓")

def test_concurrent_operations(bitcask: Bitcask):
    """测试并发操作"""
    print("\n=== 测试并发操作 ===")
    
    # 生成测试数据
    test_keys = generate_key_series(prefix="concurrent", count=1000)
    
    def concurrent_write(key):
        try:
            value = generate_random_data(record_count=1)
            bitcask.put(key, value)
            return key, value, None
        except Exception as e:
            return key, None, str(e)
    
    # 并发写入测试
    print("\n1. 并发写入测试:")
    results = {}
    errors = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        with measure_time() as concurrent_time:
            futures = [executor.submit(concurrent_write, key) for key in test_keys]
            for future in futures:
                key, value, error = future.result()
                if error:
                    errors.append((key, error))
                else:
                    results[key] = value
    
    throughput = calculate_throughput(len(test_keys), concurrent_time.duration)
    print(f"并发写入耗时: {concurrent_time.duration:.2f}秒")
    print(f"写入吞吐量: {throughput:.2f} ops/s")
    print(f"成功操作: {len(results)}")
    print(f"失败操作: {len(errors)}")
    
    if errors:
        print("\n首个错误示例:")
        print(f"Key: {errors[0][0]}")
        print(f"Error: {errors[0][1]}")

def test_large_values(bitcask: Bitcask):
    """测试大数据量操作"""
    print("\n=== 测试大数据量操作 ===")
    
    sizes = [1024, 10240, 102400]  # 1KB, 10KB, 100KB
    for size in sizes:
        key = f"large_value_{size}"
        value = generate_large_value(size)
        actual_size = len(json.dumps(value).encode('utf-8'))
        
        print(f"\n测试 {actual_size/1024:.1f}KB 大小的数据:")
        
        # 写入测试
        with measure_time() as write_time:
            with measure_memory() as mem_usage:
                bitcask.put(key, value)
        
        # 避免除零错误
        write_duration = max(write_time.duration, 0.000001)  # 使用最小值 1微秒
        write_throughput = actual_size / write_duration / 1024  # KB/s
        
        print(f"写入耗时: {write_duration:.2f}秒")
        print(f"写入速率: {write_throughput:.2f}KB/s")
        print(f"内存使用: {mem_usage.peak_mb:.2f}MB")
        
        # 读取测试
        with measure_time() as read_time:
            read_value = bitcask.get(key)
        
        # 避免除零错误
        read_duration = max(read_time.duration, 0.000001)  # 使用最小值 1微秒
        read_throughput = actual_size / read_duration / 1024  # KB/s
        
        print(f"读取耗时: {read_duration:.2f}秒")
        print(f"读取速率: {read_throughput:.2f}KB/s")
        
        # 数据一致性验证
        is_consistent = verify_data_consistency({key: value}, {key: read_value})
        print(f"数据一致性: {'通过' if is_consistent else '失败'}")

def test_transaction_stress(bitcask: Bitcask):
    """事务压力测试"""
    print("\n=== 事务压力测试 ===")
    
    def run_transaction(tx_id: int):
        txn = bitcask.begin_transaction()
        try:
            # 每个事务修改多个键值对
            for i in range(5):
                key = f"tx:{tx_id}:item:{i}"
                value = {
                    "tx_id": tx_id,
                    "item": i,
                    "value": random.randint(1, 1000),
                    "timestamp": int(time.time())
                }
                txn.put(key, value)
            txn.commit()
            return True, None
        except Exception as e:
            txn.rollback()
            return False, str(e)
    
    # 并发执行多个事务
    transaction_count = 100
    with ThreadPoolExecutor(max_workers=10) as executor:
        with measure_time() as tx_time:
            futures = [executor.submit(run_transaction, i) 
                      for i in range(transaction_count)]
            results = [future.result() for future in futures]
    
    successes = sum(1 for success, _ in results if success)
    failures = [(i, error) for i, (success, error) in enumerate(results) 
               if not success]
    
    print(f"事务总数: {transaction_count}")
    print(f"成功数量: {successes}")
    print(f"失败数量: {len(failures)}")
    print(f"成功率: {successes/transaction_count*100:.1f}%")
    print(f"总耗时: {tx_time.duration:.2f}秒")
    print(f"平均事务耗时: {tx_time.duration/transaction_count:.3f}秒")
    
    if failures:
        print("\n首个失败事务示例:")
        print(f"Transaction ID: {failures[0][0]}")
        print(f"Error: {failures[0][1]}")

import os
import time
import json
import shutil
import tempfile
import pytest
from typing import Any, Dict, List, Tuple

from bitcask import Bitcask, Transaction, TransactionError

class TestBitcask:
    def setup_method(self):
        """每个测试方法前创建临时目录"""
        self.test_dir = tempfile.mkdtemp()
        
        # 创建测试配置文件
        self.config_path = os.path.join(self.test_dir, 'test_config.yaml')
        data_dir = os.path.join(self.test_dir, 'data').replace('\\', '/')
        wal_dir = os.path.join(self.test_dir, 'wal').replace('\\', '/')
        
        config_content = f"""
storage:
  data_dir: {data_dir}
  max_file_size: 1073741824
  enable_compression: false
  compression_threshold: 1024

wal:
  wal_dir: {wal_dir}
  max_wal_size: 67108864
  sync_strategy: "always"
  sync_interval: 1.0
  retention_hours: 24
  auto_recovery: true

btree:
  enable_cache: true
  max_cache_size: 1000000
  cache_eviction: "lru"

transaction:
  timeout: 1
  max_retries: 3
  isolation_level: "repeatable_read"

performance:
  compaction_interval: 6
  read_buffer_size: 65536
  write_buffer_size: 65536
  enable_prefetch: true
  prefetch_size: 131072

monitoring:
  log_level: "info"
  enable_metrics: true
  metrics_interval: 60
  slow_query_log: true
  slow_query_threshold: 100
"""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            f.write(config_content)
        
        # 创建Bitcask实例
        self.bitcask = Bitcask(self.config_path)
    
    def teardown_method(self):
        """每个测试结束时清理目录"""
        try:
            self.bitcask.close()
        finally:
            # 等待一段时间以确保所有文件句柄都已关闭
            time.sleep(0.1)
            shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_basic_operations(self):
        """测试基本操作"""
        # 测试写入
        self.bitcask.put("key1", "value1")
        self.bitcask.put("key2", {"name": "test"})
        
        # 测试读取
        assert self.bitcask.get("key1") == "value1"
        assert self.bitcask.get("key2") == {"name": "test"}
        assert self.bitcask.get("non_existent") is None
        
        # 测试删除
        self.bitcask.delete("key1")
        assert self.bitcask.get("key1") is None
    
    def test_transaction(self):
        """测试事务"""
        # 开始事务
        txn = self.bitcask.begin_transaction()
        
        # 执行操作
        txn.put("key1", "value1")
        txn.put("key2", "value2")
        
        # 提交前数据不可见
        assert self.bitcask.get("key1") is None
        assert self.bitcask.get("key2") is None
        
        # 提交事务
        txn.commit()
        
        # 提交后数据可见
        assert self.bitcask.get("key1") == "value1"
        assert self.bitcask.get("key2") == "value2"
    
    def test_transaction_rollback(self):
        """测试事务回滚"""
        # 开始事务
        txn = self.bitcask.begin_transaction()
        
        # 执行操作
        txn.put("key1", "value1")
        txn.put("key2", "value2")
        
        # 回滚事务
        txn.rollback()
        
        # 数据不可见
        assert self.bitcask.get("key1") is None
        assert self.bitcask.get("key2") is None
    
    def test_transaction_timeout(self):
        """测试事务超时"""
        # 开始事务
        txn = self.bitcask.begin_transaction()
        
        # 执行操作
        txn.put("key1", "value1")
        
        # 等待超时
        time.sleep(1.1)
        
        # 提交应该失败
        with pytest.raises(TransactionError):
            txn.commit()
        
        # 数据不可见
        assert self.bitcask.get("key1") is None
    
    def test_scan_operations(self):
        """测试扫描操作"""
        # 写入测试数据
        test_data = {
            "user:1": {"name": "Alice"},
            "user:2": {"name": "Bob"},
            "user:3": {"name": "Charlie"},
            "post:1": {"title": "Hello"},
            "post:2": {"title": "World"}
        }
        
        for key, value in test_data.items():
            self.bitcask.put(key, value)
        
        # 测试前缀扫描
        user_items = list(self.bitcask.scan_prefix("user:"))
        assert len(user_items) == 3
        assert all(key.startswith("user:") for key, _ in user_items)
        
        # 测试范围扫描
        range_items = list(self.bitcask.range_scan("user:1", "user:3"))
        assert len(range_items) == 2
        assert range_items[0][0] == "user:1"
        assert range_items[1][0] == "user:2"
    
    def test_recovery(self):
        """测试数据恢复"""
        # 写入一些数据
        self.bitcask.put("key1", "value1")
        self.bitcask.put("key2", "value2")
        
        # 关闭实例
        self.bitcask.close()
        
        # 创建新实例
        self.bitcask = Bitcask(self.config_path)
        
        # 验证数据是否恢复
        assert self.bitcask.get("key1") == "value1"
        assert self.bitcask.get("key2") == "value2"

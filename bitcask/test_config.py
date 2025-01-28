import os
import pytest
import tempfile
import shutil
from typing import Any, Dict, List, Tuple
from bitcask.config import (
    ConfigManager, SyncStrategy, CacheEvictionPolicy,
    IsolationLevel, LogLevel
)

class TestConfig:
    def setup_method(self):
        """每个测试方法前创建临时目录和配置文件"""
        self.test_dir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.test_dir, 'test_config.yaml')
        
        # 创建测试配置文件
        config_content = """
storage:
  data_dir: "./data"
  max_file_size: 1073741824
  enable_compression: false
  compression_threshold: 1024

wal:
  wal_dir: "./wal"
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
  timeout: 30
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
        
        self.config_manager = ConfigManager(self.config_path)
    
    def teardown_method(self):
        """每个测试方法后清理临时目录"""
        shutil.rmtree(self.test_dir)
    
    def test_load_config(self):
        """测试配置加载"""
        config = self.config_manager.get_config()
        
        # 验证存储配置
        assert config.storage.data_dir == "./data"
        assert config.storage.max_file_size == 1073741824
        assert not config.storage.enable_compression
        assert config.storage.compression_threshold == 1024
        
        # 验证WAL配置
        assert config.wal.wal_dir == "./wal"
        assert config.wal.max_wal_size == 67108864
        assert config.wal.sync_strategy == SyncStrategy.ALWAYS
        assert config.wal.sync_interval == 1.0
        assert config.wal.retention_hours == 24
        assert config.wal.auto_recovery
        
        # 验证B-tree配置
        assert config.btree.enable_cache
        assert config.btree.max_cache_size == 1000000
        assert config.btree.cache_eviction == CacheEvictionPolicy.LRU
        
        # 验证事务配置
        assert config.transaction.timeout == 30
        assert config.transaction.max_retries == 3
        assert config.transaction.isolation_level == IsolationLevel.REPEATABLE_READ
        
        # 验证性能配置
        assert config.performance.compaction_interval == 6
        assert config.performance.read_buffer_size == 65536
        assert config.performance.write_buffer_size == 65536
        assert config.performance.enable_prefetch
        assert config.performance.prefetch_size == 131072
        
        # 验证监控配置
        assert config.monitoring.log_level == LogLevel.INFO
        assert config.monitoring.enable_metrics
        assert config.monitoring.metrics_interval == 60
        assert config.monitoring.slow_query_log
        assert config.monitoring.slow_query_threshold == 100
    
    def test_validate_config(self):
        """测试配置验证"""
        assert self.config_manager.validate_config()
    
    def test_create_directories(self):
        """测试目录创建"""
        config = self.config_manager.get_config()
        
        # 设置测试目录
        config.storage.data_dir = os.path.join(self.test_dir, "data")
        config.wal.wal_dir = os.path.join(self.test_dir, "wal")
        
        # 创建目录
        self.config_manager.create_directories()
        
        # 验证目录是否创建
        assert os.path.exists(config.storage.data_dir)
        assert os.path.exists(config.wal.wal_dir)
    
    def test_reload_config(self):
        """测试重新加载配置"""
        # 修改配置文件
        config_content = """
storage:
  data_dir: "./new_data"
  max_file_size: 2147483648
  enable_compression: true
  compression_threshold: 2048

wal:
  wal_dir: "./new_wal"
  max_wal_size: 134217728
  sync_strategy: "periodic"
  sync_interval: 2.0
  retention_hours: 48
  auto_recovery: false

btree:
  enable_cache: false
  max_cache_size: 500000
  cache_eviction: "fifo"

transaction:
  timeout: 60
  max_retries: 5
  isolation_level: "serializable"

performance:
  compaction_interval: 12
  read_buffer_size: 131072
  write_buffer_size: 131072
  enable_prefetch: false
  prefetch_size: 262144

monitoring:
  log_level: "debug"
  enable_metrics: false
  metrics_interval: 120
  slow_query_log: false
  slow_query_threshold: 200
"""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            f.write(config_content)
        
        # 重新加载配置
        self.config_manager.reload_config()
        config = self.config_manager.get_config()
        
        # 验证新配置
        assert config.storage.data_dir == "./new_data"
        assert config.storage.max_file_size == 2147483648
        assert config.storage.enable_compression
        assert config.wal.sync_strategy == SyncStrategy.PERIODIC
        assert config.btree.cache_eviction == CacheEvictionPolicy.FIFO
        assert config.transaction.isolation_level == IsolationLevel.SERIALIZABLE
        assert config.monitoring.log_level == LogLevel.DEBUG

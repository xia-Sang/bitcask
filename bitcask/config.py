import os
import yaml
from typing import Any, Dict, Optional
from dataclasses import dataclass
from enum import Enum

class SyncStrategy(Enum):
    """WAL同步策略"""
    ALWAYS = "always"
    PERIODIC = "periodic"
    NONE = "none"

class CacheEvictionPolicy(Enum):
    """缓存驱逐策略"""
    LRU = "lru"
    FIFO = "fifo"

class IsolationLevel(Enum):
    """事务隔离级别"""
    READ_UNCOMMITTED = "read_uncommitted"
    READ_COMMITTED = "read_committed"
    REPEATABLE_READ = "repeatable_read"
    SERIALIZABLE = "serializable"

class LogLevel(Enum):
    """日志级别"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

@dataclass
class StorageConfig:
    """存储配置"""
    data_dir: str
    max_file_size: int
    enable_compression: bool
    compression_threshold: int

@dataclass
class WALConfig:
    """WAL配置"""
    wal_dir: str
    max_wal_size: int
    sync_strategy: SyncStrategy
    sync_interval: float
    retention_hours: int
    auto_recovery: bool

@dataclass
class BTreeConfig:
    """B-tree配置"""
    enable_cache: bool
    max_cache_size: int
    cache_eviction: CacheEvictionPolicy

@dataclass
class TransactionConfig:
    """事务配置"""
    timeout: int
    max_retries: int
    isolation_level: IsolationLevel

@dataclass
class PerformanceConfig:
    """性能配置"""
    compaction_interval: int
    read_buffer_size: int
    write_buffer_size: int
    enable_prefetch: bool
    prefetch_size: int

@dataclass
class MonitoringConfig:
    """监控配置"""
    log_level: LogLevel
    enable_metrics: bool
    metrics_interval: int
    slow_query_log: bool
    slow_query_threshold: int

@dataclass
class BitcaskConfig:
    """Bitcask总配置"""
    storage: StorageConfig
    wal: WALConfig
    btree: BTreeConfig
    transaction: TransactionConfig
    performance: PerformanceConfig
    monitoring: MonitoringConfig

class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路径，如果为None则使用默认路径
        """
        self.config_path = config_path or os.path.join(
            os.path.dirname(__file__), 
            'config.yaml'
        )
        self.config = self.load_config()
    
    def load_config(self) -> BitcaskConfig:
        """
        加载配置文件
        
        Returns:
            BitcaskConfig对象
        """
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config_dict = yaml.safe_load(f)
        
        return BitcaskConfig(
            storage=StorageConfig(
                data_dir=config_dict['storage']['data_dir'],
                max_file_size=config_dict['storage']['max_file_size'],
                enable_compression=config_dict['storage']['enable_compression'],
                compression_threshold=config_dict['storage']['compression_threshold']
            ),
            wal=WALConfig(
                wal_dir=config_dict['wal']['wal_dir'],
                max_wal_size=config_dict['wal']['max_wal_size'],
                sync_strategy=SyncStrategy(config_dict['wal']['sync_strategy']),
                sync_interval=config_dict['wal']['sync_interval'],
                retention_hours=config_dict['wal']['retention_hours'],
                auto_recovery=config_dict['wal']['auto_recovery']
            ),
            btree=BTreeConfig(
                enable_cache=config_dict['btree']['enable_cache'],
                max_cache_size=config_dict['btree']['max_cache_size'],
                cache_eviction=CacheEvictionPolicy(config_dict['btree']['cache_eviction'])
            ),
            transaction=TransactionConfig(
                timeout=config_dict['transaction']['timeout'],
                max_retries=config_dict['transaction']['max_retries'],
                isolation_level=IsolationLevel(config_dict['transaction']['isolation_level'])
            ),
            performance=PerformanceConfig(
                compaction_interval=config_dict['performance']['compaction_interval'],
                read_buffer_size=config_dict['performance']['read_buffer_size'],
                write_buffer_size=config_dict['performance']['write_buffer_size'],
                enable_prefetch=config_dict['performance']['enable_prefetch'],
                prefetch_size=config_dict['performance']['prefetch_size']
            ),
            monitoring=MonitoringConfig(
                log_level=LogLevel(config_dict['monitoring']['log_level']),
                enable_metrics=config_dict['monitoring']['enable_metrics'],
                metrics_interval=config_dict['monitoring']['metrics_interval'],
                slow_query_log=config_dict['monitoring']['slow_query_log'],
                slow_query_threshold=config_dict['monitoring']['slow_query_threshold']
            )
        )
    
    def get_config(self) -> BitcaskConfig:
        """
        获取配置对象
        
        Returns:
            BitcaskConfig对象
        """
        return self.config
    
    def reload_config(self) -> None:
        """重新加载配置文件"""
        self.config = self.load_config()
    
    def validate_config(self) -> bool:
        """
        验证配置是否有效
        
        Returns:
            配置是否有效
        """
        config = self.config
        
        # 验证目录路径
        if not os.path.isabs(config.storage.data_dir):
            config.storage.data_dir = os.path.abspath(config.storage.data_dir)
        if not os.path.isabs(config.wal.wal_dir):
            config.wal.wal_dir = os.path.abspath(config.wal.wal_dir)
        
        # 验证文件大小限制
        if config.storage.max_file_size <= 0:
            return False
        if config.wal.max_wal_size <= 0:
            return False
        
        # 验证缓冲区大小
        if config.performance.read_buffer_size <= 0:
            return False
        if config.performance.write_buffer_size <= 0:
            return False
        
        # 验证时间间隔
        if config.wal.sync_strategy == SyncStrategy.PERIODIC and config.wal.sync_interval <= 0:
            return False
        if config.performance.compaction_interval <= 0:
            return False
        
        return True
    
    def create_directories(self) -> None:
        """创建必要的目录"""
        os.makedirs(self.config.storage.data_dir, exist_ok=True)
        os.makedirs(self.config.wal.wal_dir, exist_ok=True)
    
    def __str__(self) -> str:
        """返回配置的字符串表示"""
        return yaml.dump(self.config, default_flow_style=False)
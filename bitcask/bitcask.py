import os
import time
import json
import threading
import tempfile
from typing import Any, Optional, List, Tuple, Dict, Iterator
from enum import Enum
from dataclasses import dataclass

from .config import ConfigManager, BitcaskConfig
from .wal.wal import WALManager, WALEntry, WALEntryType
from .btree.btree import BTreeIndex

class BitcaskError(Exception):
    """Bitcask 错误基类"""
    pass

class TransactionError(BitcaskError):
    """事务相关错误"""
    pass

class CompactionError(BitcaskError):
    """压缩相关错误"""
    pass

@dataclass
class KeyValueEntry:
    """键值对条目"""
    key: str
    value: Any
    timestamp: float
    is_deleted: bool = False

class TransactionState(Enum):
    """事务状态"""
    ACTIVE = "active"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"

class Transaction:
    """事务类"""
    def __init__(self, bitcask: 'Bitcask', timeout: int):
        self.bitcask = bitcask
        self.timeout = timeout
        self.start_time = time.time()
        self.state = TransactionState.ACTIVE
        self.operations: List[Tuple[WALEntryType, str, Any]] = []
    
    def put(self, key: str, value: Any) -> None:
        """写入键值对"""
        if self.state != TransactionState.ACTIVE:
            raise TransactionError("Transaction is not active")
        if time.time() - self.start_time > self.timeout:
            raise TransactionError("Transaction timeout")
        
        self.operations.append((WALEntryType.PUT, key, value))
    
    def delete(self, key: str) -> None:
        """删除键值对"""
        if self.state != TransactionState.ACTIVE:
            raise TransactionError("Transaction is not active")
        if time.time() - self.start_time > self.timeout:
            raise TransactionError("Transaction timeout")
        
        self.operations.append((WALEntryType.DELETE, key, None))
    
    def commit(self) -> None:
        """提交事务"""
        if self.state != TransactionState.ACTIVE:
            raise TransactionError("Transaction is not active")
        if time.time() - self.start_time > self.timeout:
            raise TransactionError("Transaction timeout")

        # 写入WAL
        tx_id = self.bitcask.wal_manager.begin_transaction()
        try:
            for op_type, key, value in self.operations:
                if op_type == WALEntryType.PUT:
                    self.bitcask._put(key, value)
                else:
                    self.bitcask._delete(key)
            self.bitcask.wal_manager.commit_transaction(tx_id)
            self.state = TransactionState.COMMITTED
        except Exception as e:
            self.bitcask.wal_manager.rollback_transaction(tx_id)
            raise e
    
    def rollback(self) -> None:
        """回滚事务"""
        if self.state != TransactionState.ACTIVE:
            raise TransactionError("Transaction is not active")
        
        self.state = TransactionState.ROLLED_BACK
        self.operations.clear()

class Bitcask:
    """Bitcask 存储引擎"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化 Bitcask 实例
        
        Args:
            config_path: 配置文件路径，如果为None则使用默认路径
        """
        # 加载配置
        self.config_manager = ConfigManager(config_path)
        if not self.config_manager.validate_config():
            raise ValueError("Invalid configuration")
        
        self.config: BitcaskConfig = self.config_manager.get_config()
        
        # 创建必要的目录
        self.config_manager.create_directories()
        
        # 初始化组件
        self.wal_manager = WALManager(
            wal_dir=self.config.wal.wal_dir,
            max_size=self.config.wal.max_wal_size,
            sync_strategy=self.config.wal.sync_strategy
        )
        
        self.index = BTreeIndex()
        
        # 加载现有数据
        self._load_data()
        
        # 启动后台任务
        self._start_background_tasks()
    
    def _load_data(self) -> None:
        """加载现有数据"""
        # 从WAL恢复数据
        if self.config.wal.auto_recovery:
            for entry in self.wal_manager.recover():
                if entry.type == WALEntryType.PUT:
                    self.index[entry.key] = entry.value
                elif entry.type == WALEntryType.DELETE:
                    if entry.key in self.index:
                        del self.index[entry.key]
    
    def _start_background_tasks(self) -> None:
        """启动后台任务"""
        # 启动压缩任务
        if self.config.performance.compaction_interval > 0:
            self._compaction_thread = threading.Thread(
                target=self._run_compaction,
                daemon=True
            )
            self._compaction_thread.start()
    
    def _run_compaction(self) -> None:
        """运行压缩任务"""
        while True:
            try:
                # 创建检查点
                old_wal = self.wal_manager.checkpoint()
                
                # 清理过期的WAL文件
                retention_time = time.time() - (self.config.wal.retention_hours * 3600)
                self.wal_manager.cleanup(retention_time)
                
                # 等待下一次压缩
                time.sleep(self.config.performance.compaction_interval * 3600)
            except Exception as e:
                print(f"Compaction error: {e}")
                time.sleep(60)  # 发生错误时等待1分钟再重试
    
    def _put(self, key: str, value: Any) -> None:
        """
        内部写入方法
        
        Args:
            key: 键
            value: 值
        """
        # 写入WAL
        self.wal_manager.append(WALEntry(WALEntryType.PUT, key, value))
        
        # 更新索引
        self.index[key] = value
    
    def _delete(self, key: str) -> None:
        """
        内部删除方法
        
        Args:
            key: 要删除的键
        """
        # 写入WAL
        self.wal_manager.append(WALEntry(WALEntryType.DELETE, key, None))
        
        # 更新索引
        if key in self.index:
            del self.index[key]
    
    def put(self, key: str, value: Any) -> None:
        """
        写入键值对
        
        Args:
            key: 键
            value: 值
        """
        self._put(key, value)
    
    def get(self, key: str) -> Optional[Any]:
        """
        获取键对应的值
        
        Args:
            key: 键
        
        Returns:
            键对应的值，如果键不存在则返回None
        """
        return self.index.get(key)
    
    def delete(self, key: str) -> None:
        """
        删除键值对
        
        Args:
            key: 要删除的键
        """
        self._delete(key)
    
    def begin_transaction(self) -> Transaction:
        """
        开始一个新事务
        
        Returns:
            Transaction对象
        """
        return Transaction(self, self.config.transaction.timeout)
    
    def scan_prefix(self, prefix: str) -> Iterator[Tuple[str, Any]]:
        """
        前缀扫描
        
        Args:
            prefix: 键前缀
        
        Returns:
            匹配前缀的键值对迭代器
        """
        return self.index.items(prefix)
    
    def range_scan(self, start_key: str, end_key: str) -> Iterator[Tuple[str, Any]]:
        """
        范围扫描
        
        Args:
            start_key: 起始键（包含）
            end_key: 结束键（不包含）
        
        Returns:
            范围内的键值对迭代器
        """
        return self.index.range_scan(start_key, end_key)
    
    def close(self) -> None:
        """关闭Bitcask实例"""
        # 创建最终检查点
        self.wal_manager.checkpoint()
        
        # 关闭组件
        self.wal_manager.close()
        self.index.close()
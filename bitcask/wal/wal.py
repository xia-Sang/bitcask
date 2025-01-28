import os
import json
import time
import uuid
import struct
from enum import Enum
from typing import Any, Optional, List, Iterator, Dict, Tuple, Set
from dataclasses import dataclass

from ..config import SyncStrategy

class WALEntryType(Enum):
    """WAL条目类型"""
    PUT = 1        # 写入操作
    DELETE = 2     # 删除操作
    CHECKPOINT = 3 # 检查点（用于标记B-tree完整持久化点）
    TX_BEGIN = 4   # 事务开始
    TX_COMMIT = 5  # 事务提交
    TX_ROLLBACK = 6 # 事务回滚

@dataclass
class WALEntry:
    """WAL日志条目"""
    type: WALEntryType
    key: str
    value: Optional[Any] = None
    timestamp: float = 0.0
    tx_id: Optional[str] = None  # 事务ID
    
    def serialize(self) -> bytes:
        """将WAL条目序列化为字节"""
        data = {
            "type": self.type.value,
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp or time.time(),
            "tx_id": self.tx_id
        }
        json_data = json.dumps(data)
        # 格式：长度(4字节) + JSON数据
        length = len(json_data)
        return struct.pack("!I", length) + json_data.encode('utf-8')
    
    @classmethod
    def deserialize(cls, data: bytes) -> Tuple['WALEntry', int]:
        """从字节反序列化WAL条目"""
        length = struct.unpack("!I", data[:4])[0]
        json_data = json.loads(data[4:4+length].decode('utf-8'))
        entry = cls(
            type=WALEntryType(json_data["type"]),
            key=json_data["key"],
            value=json_data["value"],
            timestamp=json_data["timestamp"],
            tx_id=json_data.get("tx_id")
        )
        return entry, 4 + length

class Transaction:
    """事务类"""
    def __init__(self, tx_id: str):
        self.tx_id = tx_id
        self.operations: List[WALEntry] = []
        self.start_time = time.time()
        self.status = "active"  # active, committed, rolled_back
    
    def add_operation(self, entry: WALEntry):
        """添加操作到事务"""
        entry.tx_id = self.tx_id
        self.operations.append(entry)

class WALManager:
    """WAL管理器"""
    
    def __init__(self, wal_dir: str, max_size: int = 67108864, sync_strategy: SyncStrategy = SyncStrategy.ALWAYS):
        """
        初始化WAL管理器
        
        Args:
            wal_dir: WAL文件存储目录
            max_size: WAL文件最大大小（字节），默认64MB
            sync_strategy: 同步策略，可选值：always（每次写入都同步）, interval（定时同步）
        """
        self.wal_dir = wal_dir
        self.max_size = max_size
        self.sync_strategy = sync_strategy
        self.current_wal = None
        self.current_file = None
        self.active_transactions: Dict[str, Transaction] = {}
        self.committed_transactions: Set[str] = set()
        self._ensure_dir_exists()
        self._init_current_wal()
    
    def _ensure_dir_exists(self):
        """确保WAL目录存在"""
        os.makedirs(self.wal_dir, exist_ok=True)
    
    def _init_current_wal(self):
        """初始化当前WAL文件"""
        if self.current_file:
            self.current_file.close()
        
        # WAL文件名格式：timestamp.microseconds.wal
        timestamp = time.time()
        seconds = int(timestamp)
        microseconds = int((timestamp - seconds) * 1000000)
        wal_file = os.path.join(self.wal_dir, f"{seconds}.{microseconds:06d}.wal")
        self.current_wal = wal_file
        self.current_file = open(wal_file, 'ab+')
    
    def begin_transaction(self) -> str:
        """
        开始一个新事务
        
        Returns:
            事务ID
        """
        tx_id = str(uuid.uuid4())
        tx = Transaction(tx_id)
        self.active_transactions[tx_id] = tx
        
        # 记录事务开始
        entry = WALEntry(
            type=WALEntryType.TX_BEGIN,
            key="tx_begin",
            tx_id=tx_id
        )
        self.append(entry)
        return tx_id
    
    def commit_transaction(self, tx_id: str) -> bool:
        """
        提交事务
        
        Args:
            tx_id: 事务ID
            
        Returns:
            是否提交成功
        """
        if tx_id not in self.active_transactions:
            return False
        
        tx = self.active_transactions[tx_id]
        
        # 记录所有事务操作
        for entry in tx.operations:
            self.append(entry)
        
        # 记录事务提交
        commit_entry = WALEntry(
            type=WALEntryType.TX_COMMIT,
            key="tx_commit",
            tx_id=tx_id
        )
        self.append(commit_entry)
        
        # 更新事务状态
        tx.status = "committed"
        self.committed_transactions.add(tx_id)
        del self.active_transactions[tx_id]
        return True
    
    def rollback_transaction(self, tx_id: str) -> bool:
        """
        回滚事务
        
        Args:
            tx_id: 事务ID
            
        Returns:
            是否回滚成功
        """
        if tx_id not in self.active_transactions:
            return False
        
        # 记录事务回滚
        entry = WALEntry(
            type=WALEntryType.TX_ROLLBACK,
            key="tx_rollback",
            tx_id=tx_id
        )
        self.append(entry)
        
        # 更新事务状态
        tx = self.active_transactions[tx_id]
        tx.status = "rolled_back"
        del self.active_transactions[tx_id]
        return True
    
    def append(self, entry: WALEntry) -> bool:
        """
        追加WAL条目
        
        Args:
            entry: WAL条目
            
        Returns:
            是否追加成功
        """
        try:
            # 检查当前文件大小
            if self.current_file and os.path.getsize(self.current_wal) >= self.max_size:
                self._init_current_wal()
            
            data = entry.serialize()
            self.current_file.write(data)
            self.current_file.flush()
            
            # 根据同步策略决定是否立即同步到磁盘
            if self.sync_strategy == SyncStrategy.ALWAYS:
                os.fsync(self.current_file.fileno())
            
            return True
        except Exception as e:
            print(f"Failed to append WAL entry: {e}")
            return False
    
    def append_to_transaction(self, tx_id: str, entry: WALEntry) -> bool:
        """
        将操作添加到事务中
        
        Args:
            tx_id: 事务ID
            entry: WAL条目
            
        Returns:
            是否添加成功
        """
        if tx_id not in self.active_transactions:
            return False
        
        tx = self.active_transactions[tx_id]
        tx.add_operation(entry)
        return True
    
    def read_entries(self, wal_file: str) -> List[WALEntry]:
        """
        读取指定WAL文件中的所有条目
        
        Args:
            wal_file: WAL文件路径
            
        Returns:
            WAL条目列表
        """
        entries = []
        with open(wal_file, 'rb') as f:
            data = f.read()
            offset = 0
            while offset < len(data):
                entry, size = WALEntry.deserialize(data[offset:])
                entries.append(entry)
                offset += size
        return entries
    
    def recover(self) -> List[WALEntry]:
        """
        恢复所有WAL文件中的条目，处理未完成的事务
        
        Returns:
            需要重放的WAL条目列表，按时间戳排序
        """
        all_entries = []
        incomplete_transactions: Dict[str, List[WALEntry]] = {}
        committed_transactions: Set[str] = set()
        rolled_back_transactions: Set[str] = set()
        
        # 读取所有WAL文件
        wal_files = sorted([
            f for f in os.listdir(self.wal_dir)
            if f.endswith('.wal')
        ])
        
        for wal_file in wal_files:
            file_path = os.path.join(self.wal_dir, wal_file)
            entries = self.read_entries(file_path)
            
            for entry in entries:
                if entry.tx_id:
                    if entry.type == WALEntryType.TX_BEGIN:
                        incomplete_transactions[entry.tx_id] = []
                    elif entry.type == WALEntryType.TX_COMMIT:
                        committed_transactions.add(entry.tx_id)
                        # 将事务的所有操作添加到重放列表
                        all_entries.extend(incomplete_transactions.get(entry.tx_id, []))
                    elif entry.type == WALEntryType.TX_ROLLBACK:
                        rolled_back_transactions.add(entry.tx_id)
                        # 丢弃事务的所有操作
                        if entry.tx_id in incomplete_transactions:
                            del incomplete_transactions[entry.tx_id]
                    elif entry.tx_id in incomplete_transactions:
                        incomplete_transactions[entry.tx_id].append(entry)
                else:
                    # 非事务操作直接添加
                    all_entries.append(entry)
        
        # 按时间戳排序
        return sorted(all_entries, key=lambda x: x.timestamp)
    
    def checkpoint(self) -> str:
        """
        创建检查点，切换到新的WAL文件
        
        Returns:
            旧WAL文件的路径
        """
        # 确保所有活动事务都已经完成
        if self.active_transactions:
            raise RuntimeError("Cannot create checkpoint with active transactions")
            
        old_wal = self.current_wal
        self._init_current_wal()
        return old_wal
    
    def cleanup(self, before_timestamp: float):
        """
        清理指定时间戳之前的WAL文件
        
        Args:
            before_timestamp: 清理该时间戳之前的文件（包含该时间戳）
        """
        # 获取所有WAL文件及其时间戳
        wal_files = []
        for filename in os.listdir(self.wal_dir):
            if not filename.endswith('.wal'):
                continue
            
            try:
                # 从文件名中提取时间戳
                name_parts = filename[:-4].split('.')  # 移除.wal后缀并分割
                if len(name_parts) != 2:
                    continue
                    
                seconds = float(name_parts[0])
                microseconds = float(name_parts[1]) / 1000000
                file_timestamp = seconds + microseconds
                
                wal_files.append((filename, file_timestamp))
            except (ValueError, IndexError) as e:
                print(f"Invalid WAL filename format {filename}: {e}")
                continue
        
        # 按时间戳排序
        wal_files.sort(key=lambda x: x[1])
        
        # 删除所有小于或等于指定时间戳的文件
        for filename, timestamp in wal_files:
            if timestamp <= before_timestamp:
                file_path = os.path.join(self.wal_dir, filename)
                try:
                    os.remove(file_path)
                    print(f"Removed WAL file {filename} with timestamp {timestamp}")
                except OSError as e:
                    print(f"Failed to remove WAL file {filename}: {e}")
    
    def close(self):
        """关闭WAL管理器"""
        if self.current_file:
            self.current_file.close()
            self.current_file = None

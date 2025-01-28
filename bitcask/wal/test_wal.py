import os
import time
import shutil
import struct
import tempfile
import pytest
from typing import Any, Dict, List, Tuple

from bitcask.wal.wal import WALManager, WALEntry, WALEntryType

class TestWAL:
    def setup_method(self):
        """每个测试方法前创建临时目录"""
        self.test_dir = tempfile.mkdtemp()
        self.wal_manager = WALManager(self.test_dir)
    
    def teardown_method(self):
        """每个测试方法后清理临时目录"""
        self.wal_manager.close()
        shutil.rmtree(self.test_dir)
    
    def test_append_and_read(self):
        """测试追加和读取WAL条目"""
        # 创建测试条目
        entry1 = WALEntry(WALEntryType.PUT, "key1", "value1")
        entry2 = WALEntry(WALEntryType.DELETE, "key2")
        
        # 追加条目
        assert self.wal_manager.append(entry1)
        assert self.wal_manager.append(entry2)
        
        # 读取并验证条目
        entries = self.wal_manager.read_entries(self.wal_manager.current_wal)
        assert len(entries) == 2
        assert entries[0].type == WALEntryType.PUT
        assert entries[0].key == "key1"
        assert entries[0].value == "value1"
        assert entries[1].type == WALEntryType.DELETE
        assert entries[1].key == "key2"
        assert entries[1].value is None
    
    def test_checkpoint(self):
        """测试检查点功能"""
        # 写入一些数据
        entry = WALEntry(WALEntryType.PUT, "key1", "value1")
        self.wal_manager.append(entry)
        
        # 记录当前WAL文件
        old_wal = self.wal_manager.current_wal
        
        # 等待一小段时间确保新文件名不同
        time.sleep(0.001)
        
        # 创建检查点
        returned_wal = self.wal_manager.checkpoint()
        
        # 验证创建了新的WAL文件
        assert returned_wal == old_wal  # checkpoint返回旧文件的路径
        assert self.wal_manager.current_wal != old_wal  # 当前文件是新创建的
        assert os.path.exists(old_wal)  # 旧文件依然存在
        assert os.path.exists(self.wal_manager.current_wal)  # 新文件已创建
    
    def test_recovery(self):
        """测试恢复功能"""
        # 创建多个WAL文件并写入数据
        entries = [
            WALEntry(WALEntryType.PUT, "key1", "value1", timestamp=1.0),
            WALEntry(WALEntryType.PUT, "key2", "value2", timestamp=2.0),
        ]
        
        for entry in entries:
            self.wal_manager.append(entry)
            time.sleep(0.1)  # 确保时间戳不同
            self.wal_manager.checkpoint()
        
        # 再写入一些数据
        self.wal_manager.append(WALEntry(
            WALEntryType.DELETE, "key1", timestamp=3.0
        ))
        
        # 恢复并验证
        recovered = self.wal_manager.recover()
        assert len(recovered) == 3
        assert recovered[0].timestamp < recovered[1].timestamp
        assert recovered[1].timestamp < recovered[2].timestamp
        assert recovered[2].type == WALEntryType.DELETE
    
    def test_cleanup(self):
        """测试清理旧WAL文件"""
        # 创建多个WAL文件
        entries = [
            WALEntry(WALEntryType.PUT, f"key{i}", f"value{i}")
            for i in range(3)
        ]
        
        created_files = []
        for entry in entries:
            self.wal_manager.append(entry)
            created_files.append(self.wal_manager.current_wal)
            time.sleep(0.1)  # 确保时间戳有足够差异
            self.wal_manager.checkpoint()
        
        # 获取中间文件的时间戳
        middle_file = created_files[1]
        name_parts = os.path.basename(middle_file)[:-4].split('.')
        middle_timestamp = float(name_parts[0]) + float(name_parts[1]) / 1000000
        
        # 清理中间时间戳之前的文件
        self.wal_manager.cleanup(middle_timestamp)
        
        # 验证文件数量
        remaining_files = sorted([
            f for f in os.listdir(self.test_dir)
            if f.endswith('.wal')
        ])
        assert len(remaining_files) == 2, f"Expected 2 files, got {len(remaining_files)}: {remaining_files}"
        
        # 验证剩余文件的时间戳都大于middle_timestamp
        for filename in remaining_files:
            name_parts = filename[:-4].split('.')
            file_timestamp = float(name_parts[0]) + float(name_parts[1]) / 1000000
            assert file_timestamp > middle_timestamp, f"File {filename} has timestamp {file_timestamp} <= {middle_timestamp}"

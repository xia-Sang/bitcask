import pytest
from bitcask.btree.btree import BTreeIndex

def test_btree_basic_operations():
    """测试B树基本操作"""
    index = BTreeIndex()
    
    # 测试插入和查询
    index.put("key1", "value1")
    assert index.get("key1") == "value1"
    
    # 测试更新
    index.put("key1", "value2")
    assert index.get("key1") == "value2"
    
    # 测试删除
    index.delete("key1")
    assert index.get("key1") is None
    
def test_btree_batch_insert():
    """测试批量插入"""
    index = BTreeIndex()
    items = [
        ("key1", "value1"),
        ("key2", "value2"),
        ("key3", "value3")
    ]
    index.batch_insert(items)
    
    assert index.get("key1") == "value1"
    assert index.get("key2") == "value2"
    assert index.get("key3") == "value3"
    
def test_btree_contains():
    """测试键存在性检查"""
    index = BTreeIndex()
    index.put("key1", "value1")
    
    assert "key1" in index
    assert "key2" not in index
    
def test_btree_item_operations():
    """测试字典风格操作"""
    index = BTreeIndex()
    
    # 测试设置和获取
    index["key1"] = "value1"
    assert index["key1"] == "value1"
    
    # 测试删除
    del index["key1"]
    with pytest.raises(KeyError):
        _ = index["key1"]
        
def test_btree_iteration():
    """测试迭代"""
    index = BTreeIndex()
    items = {
        "key1": "value1",
        "key2": "value2",
        "key3": "value3"
    }
    
    for key, value in items.items():
        index[key] = value
        
    # 测试长度
    assert len(index) == len(items)
    
    # 测试迭代
    keys = set(index)
    assert keys == set(items.keys())
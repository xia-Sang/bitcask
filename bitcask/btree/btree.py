from BTrees.OOBTree import OOBTree
from typing import Any, List, Tuple, Iterator

class BTreeIndex:
    """B树索引实现"""
    
    def __init__(self):
        """初始化B树索引"""
        self._tree = OOBTree()

    def get(self, key: str) -> Any:
        """获取键对应的值"""
        return self._tree.get(key)
    
    def put(self, key: str, value: Any) -> None:
        """插入或更新键值对"""
        self._tree[key] = value
        
    def delete(self, key: str) -> None:
        """删除键值对"""
        if key in self._tree:
            del self._tree[key]
            
    def batch_insert(self, items: List[Tuple[str, Any]]) -> None:
        """批量插入键值对"""
        for key, value in items:
            self._tree[key] = value
            
    def __contains__(self, key: str) -> bool:
        """判断键是否存在"""
        return key in self._tree
    
    def __getitem__(self, key: str) -> Any:
        """获取键对应的值"""
        return self._tree[key]
    
    def __setitem__(self, key: str, value: Any) -> None:
        """设置键值对"""
        self._tree[key] = value
        
    def __delitem__(self, key: str) -> None:
        """删除键值对"""
        del self._tree[key]
        
    def __len__(self) -> int:
        """返回键值对数量"""
        return len(self._tree)
        
    def __iter__(self) -> Iterator[str]:
        """返回键的迭代器"""
        return iter(self._tree)

    def items(self, prefix: str = "") -> Iterator[Tuple[str, Any]]:
        """返回所有键值对的迭代器，可选按前缀过滤"""
        if not prefix:
            yield from self._tree.items()
        else:
            for key, value in self._tree.items():
                if key.startswith(prefix):
                    yield key, value

    def range_scan(self, start_key: str, end_key: str) -> Iterator[Tuple[str, Any]]:
        """返回指定范围内的键值对的迭代器"""
        for key, value in self._tree.items():
            if start_key <= key < end_key:
                yield key, value

    def close(self) -> None:
        """关闭索引"""
        self._tree = None

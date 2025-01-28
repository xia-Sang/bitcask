import random
import string
import json
from typing import Dict, Any, List

def generate_random_string(length: int) -> str:
    """生成指定长度的随机字符串"""
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

def generate_random_data(record_count: int = 1000, 
                        min_key_length: int = 8,
                        max_key_length: int = 16,
                        min_value_size: int = 100,
                        max_value_size: int = 1000) -> Dict[str, Any]:
    """
    生成随机测试数据
    
    Args:
        record_count: 要生成的记录数量
        min_key_length: 最小键长度
        max_key_length: 最大键长度
        min_value_size: 最小值大小(字节)
        max_value_size: 最大值大小(字节)
    
    Returns:
        包含随机键值对的字典
    """
    data = {}
    for _ in range(record_count):
        # 生成随机键
        key_length = random.randint(min_key_length, max_key_length)
        key = generate_random_string(key_length)
        
        # 生成随机值
        value = {
            "id": str(random.randint(1, 1000000)),
            "timestamp": random.randint(1600000000, 1700000000),
            "data": generate_random_string(
                random.randint(min_value_size, max_value_size)
            ),
            "metadata": {
                "type": random.choice(["user", "order", "product", "log"]),
                "version": f"{random.randint(1, 5)}.{random.randint(0, 9)}",
                "tags": [
                    generate_random_string(5) 
                    for _ in range(random.randint(1, 5))
                ]
            }
        }
        data[key] = value
    
    return data

def generate_key_series(prefix: str, count: int, 
                       start_id: int = 1) -> List[str]:
    """
    生成一系列有序的键
    
    Args:
        prefix: 键前缀
        count: 要生成的键数量
        start_id: 起始ID
    
    Returns:
        键列表
    """
    return [f"{prefix}:{i}" for i in range(start_id, start_id + count)]

def generate_large_value(size_bytes: int) -> Dict[str, Any]:
    """
    生成指定大小的大型数据值
    
    Args:
        size_bytes: 目标数据大小(字节)
    
    Returns:
        包含随机数据的字典
    """
    # 计算需要生成的随机字符串长度
    # 考虑到JSON编码和其他字段的开销，预留一些空间
    overhead = 200  # 预估的JSON结构开销
    data_length = max(0, size_bytes - overhead)
    
    value = {
        "id": str(random.randint(1, 1000000)),
        "timestamp": random.randint(1600000000, 1700000000),
        "type": "large_data",
        "content": generate_random_string(data_length),
        "checksum": generate_random_string(32)  # 模拟SHA-256校验和
    }
    
    # 验证生成的数据大小
    actual_size = len(json.dumps(value).encode('utf-8'))
    if actual_size < size_bytes:
        # 如果小于目标大小，补充随机数据
        value["padding"] = generate_random_string(size_bytes - actual_size)
    
    return value

def generate_sequential_data(start_key: int, 
                           end_key: int,
                           prefix: str = "seq") -> Dict[str, Any]:
    """
    生成连续的测试数据
    
    Args:
        start_key: 起始键值
        end_key: 结束键值
        prefix: 键前缀
    
    Returns:
        包含连续键值对的字典
    """
    data = {}
    for i in range(start_key, end_key + 1):
        key = f"{prefix}:{i:010d}"
        value = {
            "id": i,
            "sequence": i - start_key,
            "data": f"Sequential data for key {i}",
            "metadata": {
                "type": "sequential",
                "index": i,
                "timestamp": 1600000000 + i
            }
        }
        data[key] = value
    return data

def generate_test_dataset(dataset_type: str = "mixed", 
                         size: int = 1000) -> Dict[str, Any]:
    """
    生成特定类型的测试数据集
    
    Args:
        dataset_type: 数据集类型 ("mixed", "user", "order", "product")
        size: 数据集大小
    
    Returns:
        测试数据集字典
    """
    if dataset_type == "user":
        return generate_user_dataset(size)
    elif dataset_type == "order":
        return generate_order_dataset(size)
    elif dataset_type == "product":
        return generate_product_dataset(size)
    else:  # mixed
        return {
            **generate_user_dataset(size // 3),
            **generate_order_dataset(size // 3),
            **generate_product_dataset(size // 3)
        }

def generate_user_dataset(size: int) -> Dict[str, Any]:
    """生成用户数据集"""
    data = {}
    for i in range(size):
        key = f"user:{i:010d}"
        value = {
            "id": i,
            "username": generate_random_string(8),
            "email": f"{generate_random_string(8)}@example.com",
            "age": random.randint(18, 80),
            "created_at": random.randint(1600000000, 1700000000)
        }
        data[key] = value
    return data

def generate_order_dataset(size: int) -> Dict[str, Any]:
    """生成订单数据集"""
    data = {}
    for i in range(size):
        key = f"order:{i:010d}"
        value = {
            "id": i,
            "user_id": random.randint(1, 1000),
            "amount": round(random.uniform(10.0, 1000.0), 2),
            "status": random.choice(["pending", "paid", "shipped", "completed"]),
            "created_at": random.randint(1600000000, 1700000000)
        }
        data[key] = value
    return data

def generate_product_dataset(size: int) -> Dict[str, Any]:
    """生成产品数据集"""
    data = {}
    for i in range(size):
        key = f"product:{i:010d}"
        value = {
            "id": i,
            "name": f"Product-{generate_random_string(8)}",
            "price": round(random.uniform(1.0, 999.99), 2),
            "stock": random.randint(0, 1000),
            "category": random.choice(["electronics", "clothing", "food", "books"]),
            "created_at": random.randint(1600000000, 1700000000)
        }
        data[key] = value
    return data

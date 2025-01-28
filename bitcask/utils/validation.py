from typing import Dict, Any, List, Tuple
import json

def verify_data_consistency(original: Dict[str, Any], 
                          retrieved: Dict[str, Any]) -> bool:
    """验证数据一致性"""
    if original.keys() != retrieved.keys():
        return False
    
    return all(
        json.dumps(original[key], sort_keys=True) == 
        json.dumps(retrieved[key], sort_keys=True)
        for key in original
    )

def compare_data_sets(set1: Dict[str, Any], 
                     set2: Dict[str, Any]) -> Tuple[List[str], List[str], List[str]]:
    """
    比较两个数据集的差异
    
    Returns:
        Tuple[missing_keys, extra_keys, different_values]
    """
    keys1 = set(set1.keys())
    keys2 = set(set2.keys())
    
    missing_keys = list(keys1 - keys2)
    extra_keys = list(keys2 - keys1)
    
    common_keys = keys1 & keys2
    different_values = [
        key for key in common_keys
        if json.dumps(set1[key], sort_keys=True) != 
           json.dumps(set2[key], sort_keys=True)
    ]
    
    return missing_keys, extra_keys, different_values 
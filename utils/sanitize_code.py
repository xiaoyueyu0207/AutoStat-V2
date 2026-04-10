def sanitize_code(code):
    """
    清理和标准化代码字符串
    
    Args:
        code: 原始代码字符串
    
    Returns:
        清理后的代码字符串
    """
    # 移除代码前后的空白字符
    code = code.strip()
    
    # 移除代码中的 Markdown 代码块标记
    if code.startswith('```python'):
        code = code[9:]
    elif code.startswith('```'):
        code = code[3:]
    
    if code.endswith('```'):
        code = code[:-3]
    
    # 移除代码前后的空白字符
    code = code.strip()
    
    return code


def to_json_serializable(obj):
    """
    将对象转换为可JSON序列化的类型
    
    Args:
        obj: 要转换的对象
    
    Returns:
        可JSON序列化的对象
    """
    import numpy as np
    import pandas as pd
    
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict()
    elif isinstance(obj, pd.Series):
        return obj.to_dict()
    elif isinstance(obj, (list, tuple)):
        return [to_json_serializable(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: to_json_serializable(v) for k, v in obj.items()}
    else:
        return obj

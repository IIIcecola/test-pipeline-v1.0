import os
import subprocess
import tempfile
import json
import sys
import importlib
from typing import List, Any, Tuple, Dict, Optinal

class EnvironmentManager:
    """环境管理工具类，负责处理虚拟环境激活和命令执行，支持conda环境"""
    
    @staticmethod
    def get_activate_command(venv_path: str) -> str:
        """获取激活虚拟环境的命令字符串，支持conda环境和普通virtualenv"""
        if not venv_path or not os.path.exists(venv_path):
            return []
        
        # 检测是否为conda环境
        is_conda_env = os.path.exists(os.path.join(venv_path, 'conda-meta'))
        
        if os.name == 'nt':  # Windows系统
            if is_conda_env:
                return f'conda activate "{venv_path}" &&'
            else:
                activate_script = os.path.join(venv_path, 'Scripts', 'activate.bat')
                return f'"{activate_script}" &&'
                
        else:  # Linux/Mac系统
            if is_conda_env:
                return f'. $(conda info --base)/etc/profile.d/conda.sh && conda activate "{venv_path}" &&'
            else:
                activate_script = os.path.join(venv_path, 'bin', 'activate')
                return f'source "{activate_script}" &&'

    @staticmethod
    def run_in_environment(venv_path: str, command: List[str], input_data: Any = None) -> Tuple[Any, str]:
        """正确执行命令字符串，确保通过shell解析"""
        activate_cmd = EnvironmentManager.get_activate_command(venv_path)
        print("activate_cmd: ", activate_cmd)
        
        # 创建临时文件
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            json.dump(input_data, f)
            input_file = f.name
        
        tmp = tempfile.NamedTemporaryFile(suffix='.json', delete=False)
        output_file = tmp.name
        
        try:
            # 构建命令部分
            command_parts = command + [input_file, output_file]
            command_str = ' '.join(f'{part}' for part in command_parts)
            print("command_str: ", command_str)
            
            # 组合完整命令
            full_command = f"{activate_cmd} {command_str}" if activate_cmd else command_str
            # print(f"执行命令: {full_command}")  # 调试用
            
            # 关键修复：正确配置subprocess参数
            result = subprocess.run(
                full_command,
                shell=True,  # 必须设为True，让shell解析命令
                capture_output=True,
                text=True,
                check=True
            )
            
            # 读取输出结果
            with open(output_file, 'r') as f:
                output_data = json.load(f)

            print(f"subprocess_result: {output_data}")
            return output_data, None
            
        except subprocess.CalledProcessError as e:
            return None, f"命令执行失败: 退出码 {e.returncode}, 错误: {e.stderr}, 命令: {full_command}"
        except Exception as e:
            return None, f"处理过程出错: {str(e)}, 命令: {full_command}"
        finally:
            # 清理临时文件
            if os.path.exists(input_file):
                os.remove(input_file)
            if os.path.exists(output_file):
                os.remove(output_file)

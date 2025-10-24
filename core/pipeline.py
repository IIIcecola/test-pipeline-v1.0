import json
import os
import sys
import subprocess
import tempfile
import importlib
import inspect
import traceback
from typing import Dict, List, Any, Optional, Tuple

from data_clean.video_duration_filter_pipeline import VideoDurationFilter
from data_process.blur_pipeline import BlurDetector
from utils.venv import  EnvironmentManager
from utils.utils import write_processing_log

class DataProcessingPipeline:
    def __init__(self, config_path: Optional[str] = None):
        self.modules: Dict[str, Dict[str, Any]] = {}  
        self.pipeline_steps: List[Dict[str, Any]] = []  
        self.results: Dict[str, Any] = {}  
        self.config: Dict[str, Any] = { 
            "pipeline_name": "data process pipeline",
            "stop_on_error": True,
            "supported_formats": ['.mp4', '.mov', '.avi', '.mkv']
        }
        
        if config_path:
            self.load_config(config_path)

    def load_config(self, config_path: str) -> None:
        """加载配置文件"""
        if not config_path:
            return
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # 更新全局配置
            self.config.update({
                "pipeline_name": config.get("pipeline_name", self.config["pipeline_name"]),
                "stop_on_error": config.get("stop_on_error", self.config["stop_on_error"]),
                "supported_formats": config.get("supported_formats", self.config["supported_formats"])
            })
            
            # 加载模块和步骤
            self._load_modules(config.get("modules", {}))
            self._load_steps(config.get("pipeline_steps", []))
            
            print(f"configuration loaded successfully: {self.config['pipeline_name']}")
            print(f"modules: {len(self.modules)}, steps: {len(self.pipeline_steps)}")
            
        except Exception as e:
            print(f"error: {str(e)}")

    def _load_modules(self, modules_config: Dict[str, Any]) -> None:
        """加载并注册模块"""
        for module_name, module_info in modules_config.items():
            module_class_name = module_info.get("class")
            if not all(k in module_info for k in ["type", "path"])
              print(f"module {module_name} not configuration incomplete")
              continue
          
            # 注册模块信息（不直接实例化，在运行时根据环境调用）
            self.register_module(
              name=module_name,
              module_info={
                "type": module_info["type"],
                "path": module_info["path"],
                "venv_path": module_info.get("venv_path"),
                "config": module_info.get("config", {})
              }
            )

    def _load_steps(self, steps_config: List[Dict[str, Any]]) -> None:
        """加载处理步骤"""
        for step in steps_config:
            try:
                self.add_step(
                    step_name=step["step_name"],
                    module_name=step["module_name"],
                    input_params=step["input_params"],
                    output_key=step.get("output_key")
                )
            except KeyError as e:
                print(f"步骤配置缺少参数 {e}，已跳过")
                traceback.print_ecx()

    def register_module(self, name: str, module_info: Dict[str, Any]) -> None:
            """注册处理模块，包含虚拟环境信息"""
            if name in self.modules:
                print(f"⚠️ 模块 {name} 已存在，将被覆盖")
            
            self.modules[name] = module_info

    def add_step(self, step_name: str, module_name: str, input_params: Dict[str, str], output_key: Optional[str] = None) -> None:
        """添加处理步骤"""
        if module_name not in self.modules:
            print(f"⚠️ 步骤 {step_name} 引用了未注册的模块 {module_name}")
        
        self.pipeline_steps.append({
            "step_name": step_name,
            "module_name": module_name,
            "input_params": input_params,
            "output_key": output_key or step_name
        })

    def run(self, input_path: str) -> Dict[str, Any]:
        """执行数据处理管线，支持在不同虚拟环境中运行模块"""
        # 检查输入路径是否存在
        if not os.path.exists(input_path):
            print(f"❌ 输入路径不存在: {input_path}")
            return {}
        
        # 获取所有待处理的视频文件
        video_files = self._get_video_files(input_path)
        if not video_files:
            print(f"❌ 未找到任何视频文件: {input_path}")
            return {}
        
        print(f"\n🚀 开始执行 {self.config['pipeline_name']}")
        print(f"📂 待处理文件数: {len(video_files)}")
        
        # 批量处理所有视频
        all_results = {}
        for idx, file_path in enumerate(video_files):
            file_name = os.path.basename(file_path)
            print(f"\n [{idx+1}/{len(video_files)}], 开始处理: {file_name}")
            
            try:
                # 处理单个视频
                single_result = self._process_single_file(file_path)
                all_results[file_name] = single_result
                print(f"处理完成: {file_name}")
            except Exception as e:
                print(f"处理 {file_name} 失败: {str(e)}")
                traceback.print_exc()
                if self.config.get("stop_on_error", True):
                    print("遇到错误，已终止批量处理")
                    break
        
        print(f"\n 批量处理完成，成功处理 {len(all_results)} 个文件")
        return all_results

    def _get_video_files(self, input_path: str) -> List[str]:
        """获取所有符合条件的视频文件路径"""
        video_files = []
        
        # 如果是单个文件
        if os.path.isfile(input_path):
            ext = os.path.splitext(input_path)[1].lower()
            if ext in self.config["supported_formats"]:
                video_files.append(input_path)
            return video_files
        
        # 如果是文件夹，遍历所有文件
        for root, _, files in os.walk(input_path):
            for file in files:
                ext = os.path.splitext(file)[1].lower()
                if ext in self.config["supported_formats"]:
                    video_files.append(os.path.join(root, file))
        
        return sorted(video_files)  # 按路径排序

    def _process_single_file(self, file_path: str) -> Dict[str, Any]:
        """处理单个视频文件，支持在指定虚拟环境中运行模块"""
        current_data: Dict[str, Any] = {"input": file_path}
        results: Dict[str, Any] = {"original_path": file_path}
        
        for step in self.pipeline_steps:
            step_name = step["step_name"]
            module_name = step["module_name"]
            
            try:
                # 获取模块信息
                if module_name not in self.modules:
                    raise ValueError(f"模块 {module_name} 未注册")
                
                module_info = self.modules[module_name]
                module_config = module_info["config"]
                
                # 准备参数
                params = {}
                for param_key, data_key in step["input_params"].items():
                    params[param_key] = current_data.get(data_key) or results.get(data_key)
                
                # 根据模块类型执行处理
                if module_info["type"] == "local":
                    # 本地模块（同一环境）
                    module_class = globals().get(module_info["path"])
                    if not module_class:
                        raise ValueError(f"未找到本地模块类 {module_info['path']}")

                    video_path = params.get("video_path").get("video_path")

                    init_params = module_config.copy()
                    init_parmas["video_path"] = video_path
                    self._validate_init_params(module_class, init_params, module_name, step_name)

                    try:
                      module_instance = module_calss(**init_params)
                    except Exception as e:
                      raise RuntimeRrror(
                        f"实例化本地模块{module_calss.__name__}失败：{str(e)}\n"
                        f"实例化参数：{init_params}"
                      )from e
                      traceback.print_exc()
                      
                    result = module_instance.process()
                    
                elif module_info["type"] == "external":
                    # 外部模块（独立环境）
                    if not os.path.exists(module_info["path"]):
                        raise ValueError(f"外部模块脚本不存在: {module_info['path']}")
                    
                    # 准备输入数据
                    input_data = {
                        "params": params,
                        "config": module_config
                    }
                    
                    # 构建命令（假设外部模块是Python脚本）list[str], 假设只需要python script.py调用
                    command = ["python", module_info["path"]]
                    
                    # 在指定环境中运行
                    venv_path = module_info.get("venv_path")
                    print(f"\n{step_name}的虚拟环境：{venv_path}")
                    result, error = EnvironmentManager.run_in_environment(
                        venv_path=venv_path,
                        command=command,
                        input_data=input_data
                    )
                    
                    if error:
                        raise ValueError(f"外部模块执行错误: {error}")
                        traceback.print_exc()
                    
                else:
                    raise ValueError(f"不支持的模块类型: {module_info['type']}")
                
                # 保存结果
                output_key = step["output_key"]
                current_data[output_key] = result
                results[output_key] = result
                
            except Exception as e:
                print(f"步骤 {step_name} 失败: {str(e)}")
                traceback.print_exc()
                if self.config.get("stop_on_error", True):
                    raise  # 抛出异常，终止当前文件处理
        
        return results

    def _validate_init_params(self, module_class: type, init_params: Dict[str, Any], module_name: str, step_name: str) -> None:
      """
      校验实例化参数是否匹配模块类的__init__签名
      作用：提前发现配置错误，避免运行时崩溃
      """
      # 1. 获取类__init__方法的参数签名
      try:
          init_signature = inspect.signature(module_class.__init__)
      except ValueError:
          # 极少数情况：类没有__init__方法（继承自object且未重写）
          return
      
      # 2. 提取__init__的参数信息（排除self）
      init_params_meta = list(init_signature.parameters.values())
      init_param_names = [p.name for p in init_params_meta if p.name != "self"]
      
      # 3. 检查必填参数是否缺失（无默认值的参数）
      required_params = []
      for param in init_params_meta:
          if (param.name == "self"):
              continue
          # 判断是否为必填参数（无默认值）
          if param.default == inspect.Parameter.empty:
              required_params.append(param.name)
      
      # 找出配置中缺失的必填参数
      missing_params = [p for p in required_params if p not in init_params]
      if missing_params:
          raise ValueError(
              f"步骤「{step_name}」的模块「{module_name}」配置缺失必填参数：{missing_params}\n"
              f"该模块类「{module_class.__name__}」的必填参数为：{required_params}\n"
              f"请在JSON配置的「{module_name}.config」中补充这些参数"
          )
      
      # 4. 检查是否存在多余参数（可选：避免配置冗余）
      extra_params = [p for p in init_params if p not in init_param_names]
      if extra_params:
          print(f"⚠️ 步骤「{step_name}」的模块「{module_name}」存在多余配置参数：{extra_params}\n"
                f"该模块类「{module_class.__name__}」仅支持参数：{init_param_names}")
      
      # 5. 检查参数类型（可选：需类的__init__有类型注解）
      for param_name, param_meta in init_signature.parameters.items():
          if (param_name == "self") or (param_name not in init_params):
              continue
          
          # 获取类__init__中该参数的预期类型（需类定义时加类型注解）
          expected_type = param_meta.annotation
          if expected_type == inspect.Parameter.empty:
              continue  # 无类型注解，跳过校验
          
          # 获取配置中的实际参数值和类型
          actual_value = init_params[param_name]
          actual_type = type(actual_value)
          
          # 校验类型（支持Union类型，如Optional[str] = None）
          if isinstance(expected_type, type):
              # 普通类型（如str、int）
              if not isinstance(actual_value, expected_type):
                  raise TypeError(
                      f"步骤「{step_name}」的模块「{module_name}」参数「{param_name}」类型错误\n"
                      f"预期类型：{expected_type.__name__}，实际类型：{actual_type.__name__}\n"
                      f"当前配置值：{actual_value}"
                  )
          else:
              # Union类型（如Optional[str]、Union[str, int]）
              try:
                  from typing import get_args, get_origin
                  if get_origin(expected_type) is Union:
                      expected_types = get_args(expected_type)
                      if not isinstance(actual_value, expected_types):
                          raise TypeError(
                              f"步骤「{step_name}」的模块「{module_name}」参数「{param_name}」类型错误\n"
                              f"预期类型：{[t.__name__ for t in expected_types]}，实际类型：{actual_type.__name__}\n"
                              f"当前配置值：{actual_value}"
                          )
              except ImportError:
                  pass  # 环境不支持typing模块，跳过Union类型校验
  
    def list_modules(self) -> None:
        """列出已注册的模块，包括虚拟环境信息"""
        print("\n📦 已注册模块:")
        for name, info in self.modules.items():
            venv_info = f"（虚拟环境: {info['venv_path']}）" if info.get('venv_path') else ""
            print(f"  - {name}: 类型={info['type']}, 路径={info['path']} {venv_info}")

    def list_steps(self) -> None:
        """列出处理步骤"""
        print("\n📝 处理步骤:")
        for i, step in enumerate(self.pipeline_steps, 1):
            print(f"  {i}. {step['step_name']} → 模块: {step['module_name']} → 输出键: {step['output_key']}")

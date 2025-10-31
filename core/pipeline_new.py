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
from utils.path_manager import PathManager

class DataProcessingPipeline:
    def __init__(self, config_path: Optional[str] = None):
        self.modules: Dict[str, Dict[str, Any]] = {}  
        self.pipeline_steps: List[Dict[str, Any]] = []  
        self.results: Dict[str, Any] = {}  
        self.config: Dict[str, Any] = { 
            "pipeline_name": "data process pipeline",
            "stop_on_error": True,
            "supported_video_formats": ['.mp4', '.mov', '.avi', '.mkv'],
            "supported_image_formats": ['.jpg', '.jpeg', '.png', '.bmp', '.gif'],
            "default_overwrite": False
        }
        
        if config_path:
            self.load_config(config_path)

        self.path_manager = PathManager(
            supported_image_formats=config.get("supported_image_formats"),
            supported_video_formats=config.get("supported_video_formats"),
            default_overwrite=config.get("default_overwrite", False)
        )

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
                "supported_video_formats": config.get("supported_video_formats", self.config["supported_video_formats"]),
                "supported_image_formats": config.get("supported_image_formats", self.config["supported_image_formats"]),
                "default_overwrite": config.get("default_overwrite", False)
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
                    bridge=step["bridge"]
                )
            except KeyError as e:
                print(f"步骤配置缺少参数 {e}，已跳过")
                traceback.print_ecx()

    def register_module(self, name: str, module_info: Dict[str, Any]) -> None:
            """注册处理模块，包含虚拟环境信息"""
            if name in self.modules:
                print(f"⚠️ 模块 {name} 已存在，将被覆盖")
            
            self.modules[name] = module_info

    def add_step(self, step_name: str, module_name: str, input_params: Dict[str, str], bridge: Dict[str, str]) -> None:
        """添加处理步骤"""
        if module_name not in self.modules:
            print(f"⚠️ 步骤 {step_name} 引用了未注册的模块 {module_name}")
        
        self.pipeline_steps.append({
            "step_name": step_name,
            "module_name": module_name,
            "input_params": input_params,
            "bridge": bridge
        })

    def run(self, input_path: str) -> Dict[str, Any]:
        """执行数据处理管线，支持按步骤批量处理"""
        if not os.path.exists(input_path):
            print(f"输入路径不存在: {input_path}")
            return {}
        
        print(f"\n开始执行 {self.config['pipeline_name']}")

        # 预分类所有文件（仅执行一次）
        classified_files = self.path_manager.classify_files(input_path)
        print(f"📊 预分类结果: 图片{len(classified_files['image'])}个, 视频{len(classified_files['video'])}
        
        # 初始化每个文件的结果字典
        current_dir = input_path
        current_classified = classified_files  # 复用预分类结果，**步骤间更新**
        all_results = {"steps": []}
   
        for step in self.config["pipeline_steps"]:
            step_name = step["step_name"]
            output_path = self.modules[step["module_name"]]["config"].get("output_path")
            bridge_config = step.get("bridge", {})
            print(f"\n===== 开始执行步骤: {step_name} =====")
    
            if not output_path:
                print(f"步骤 {step_name} 未配置output_path，跳过")
                continue
            try:
                # 处理当前步骤（批量处理）
                step_result = self._process_step(
                    step=step,
                    input_classified=current_classified,  # 直接传入分类结果（图片/视频列表）
                    output_dir=output_path,
                    bridge_config=bridge_config
                )
        
                # 更新下一步状态：重新分类当前步骤的输出目录（因为输出可能有新文件）
                current_classified = self.path_manager.classify_files(output_path)
                current_dir = output_path
                all_results["steps"].append({
                    "step_name": step_name,
                    "input_dir": current_dir,
                    "input_classified": {k: len(v) for k, v in current_classified.items()},  # 记录数量
                    "output_dir": output_path,
                    "result": step_result
                })
            except Exception as e:
                print(f"{step_name failed: {str(e)}}")
                return all_results
    
        print(f"\n批量处理完成，总步骤数: {len(all_results['steps'])}")
        return all_results

    def _process_step(self, step: Dict[str, Any], input_classified: Dict[str, List[str]], 
        output_dir: str, bridge_config: Dict[str, Any]) -> Dict[str, Any]:
        """批量处理单个步骤（桥接+模块批量处理）"""
        step_name = step["step_name"]
        module_name = step["module_name"]
        module_info = self.modules[module_name]
        module_config = module_info["config"]
        skip_types = bridge_config.get("skip_types", [])  # 跳过的类型（如["image"]）
        bridge_action = bridge_config.get("action", "copy")  # copy/move
        if bridge_action not in ["copy", "move"]:
            raise ValueError(f"步骤 {step_name} 桥接配置错误：action必须为'copy'或'move'")
    
        step_result = {
            "processed_types": [],  # 处理的类型
            "processed_count": 0,   # 处理的文件数
            "bridged": [],          # 桥接的类型及数量
            "errors": [],
            "module_details": {}
        }
    
        # 1. 处理桥接文件（批量复制/移动跳过的类型）
        for file_type in skip_types:
            if file_type not in input_classified:
                continue
            source_files = input_classified[file_type]
            if not source_files:
                continue
    
            try:
                # 批量复制/移动到输出目录（不维持子目录）
                skip_output_dir = os.path.join(output_dir, file_type)
                if bridge_action == "copy":
                    target_paths = self.path_manager.batch_copy_files(
                        source_files=source_files,
                        output_dir=output_dir
                    )
                else:
                    target_paths = self.path_manager.batch_move_files(
                        source_files=source_files,
                        output_dir=output_dir
                    )
                step_result["bridged"].append({
                    "type": file_type,
                    "count": len(source_files),
                    "action": bridge_action
                })
                print(f"桥接 {file_type} {len(source_files)} 个，动作: {bridge_action}")
            except Exception as e:
                step_result["errors"].append({
                    "type": file_type,
                    "error": str(e),
                    "stage": "bridge"
                })
    
        # 2. 处理需要执行的类型（批量输入目录给模块）
        process_types = [t for t in input_classified if t not in skip_types]
        for file_type in process_types:
            source_files = input_classified[file_type]
            if not source_files:
                continue
            module_input_dir = None
            try:
                # 准备模块输入目录（存放当前类型的所有文件）
                module_input_dir = os.path.join(output_dir, f"_{file_type}_input")  # 临时输入目录
                os.makedirs(module_input_dir, exist_ok=True)
    
                # 批量复制文件到模块输入目录（避免修改原始文件）
                self.path_manager.batch_copy_files(
                    source_files=source_files,
                    output_dir=module_input_dir
                )
    
                # 调用模块批量处理（输入为目录，模块内部批量处理）
                module_result = self._process_batch_with_module(
                    module_name=module_name,
                    module_config=module_config,
                    input_dir=module_input_dir,  # 模块输入：存放待处理文件的目录
                    step_name=step_name
                )
                step_result["module_details"][file_type] = module_result
                step_result["processed_types"].append(file_type)
                step_result["processed_count"] += len(source_files)
                print(f"批量处理 {file_type} {len(source_files)} 个，模块: {module_name}")
            except Exception as e:
                step_result["errors"].append({
                    "type": file_type,
                    "error": str(e),
                    "stage": "process"
                })
    
        return step_result

    def _process_batch_with_module(self, module_name: str, module_config: Dict, input_dir: str, step_name: str) -> Any:
        """调用模块批量处理目录（适配支持目录输入的模块）"""
        module_info = self.modules.get(module_name)
        if not module_info:
            raise ValueError(f"模块 {module_name} 未注册")
    
        # 确保模块输出目录存在
        os.makedirs(output_dir, exist_ok=True)
    
        if module_info["type"] == "local":
            # 本地模块：传入输入目录和输出目录（模块内部批量处理）
            module_class = globals().get(module_info["path"])
            if not module_class:
                raise ValueError(f"未找到本地模块类 {module_info['path']}")

            init_params = module_config.copy()
            init_params["video_path"] = video_path
            self._validate_init_params(module_class, init_params, module_name, step_name)
            
            module_instance = module_class(**init_params)
            return module_instance.process() 
    
        elif module_info["type"] == "external":
            # 外部模块：通过命令行传递输入/输出目录
            if not os.path.exists(module_info["path"]):
                raise ValueError(f"外部模块脚本不存在: {module_info['path']}")
            input_data = {
                "file_path": input_dir,
                "config": module_config
            }
            command = ["python", module_info["path"]]
            venv_path = module_info.get("venv_path")
    
            result, error = EnvironmentManager.run_in_environment(
                venv_path=venv_path,
                command=command,
                input_data=input_data
            )
            if error:
                raise ValueError(f"外部模块执行错误: {error}")
            return result
    
        else:
            raise ValueError(f"不支持的模块类型: {module_info['type']}")

    def _process_single_step(self, input_dir: str, step: Dict[str, Any]) -> Any:
        """处理单个文件的单个步骤"""
        step_name = step["step_name"]
        module_name = step["module_name"]
        input_params = step["input_params"]
      
        if module_name not in self.modules:
            raise ValueError(f"模块 {module_name} 未注册")
      
        module_info = self.modules[module_name]
        module_config = module_info["config"]
      
        # 执行模块处理（复用原有的本地/外部模块处理逻辑）
        if module_info["type"] == "local":
            module_class = globals().get(module_info["path"])
            if not module_class:
                raise ValueError(f"未找到本地模块类 {module_info['path']}")
          
            init_params = module_config.copy()
            init_params["file_path"] = input_dir
            self._validate_init_params(module_class, init_params, module_name, step_name)
          
            try:
                module_instance = module_class(**init_params)
                return module_instance.process()
            except Exception as e:
                raise RuntimeError(
                    f"本地模块 {module_class.__name__} 处理失败：{str(e)}"
                ) from e
              
        elif module_info["type"] == "external":
            if not os.path.exists(module_info["path"]):
                raise ValueError(f"外部模块脚本不存在: {module_info['path']}")
          
            input_data = {
                "file_path": input_dir,
                "config": module_config
            }
          
            command = ["python", module_info["path"]]
            venv_path = module_info.get("venv_path")
          
            result, error = EnvironmentManager.run_in_environment(
                venv_path=venv_path,
                command=command,
                input_data=input_data
            )
          
            if error:
                raise ValueError(f"外部模块执行错误: {error}")
            return result
        else:
            raise ValueError(f"不支持的模块类型: {module_info['type']}")
    
    def _get_media_files(self, input_path: str) -> List[str]: 
        """获取所有符合条件的图片和视频文件路径"""
        media_files = []
        video_extensions = self.config["supported_formats"]
        image_extensions = self.config["supported_image_formats"] 
        
        # 如果是单个文件
        if os.path.isfile(input_path):
            ext = os.path.splitext(input_path)[1].lower()
            if ext in video_extensions or ext in image_extensions:
                media_files.append(input_path)
            return media_files
        
        # 如果是文件夹，遍历所有文件
        for root, _, files in os.walk(input_path):
            for file in files:
                ext = os.path.splitext(file)[1].lower()
                if ext in video_extensions or ext in image_extensions:  
                    media_files.append(os.path.join(root, file))
        
        return sorted(media_files)  # 按路径排序

    def _process_single_file(self, file_path: str) -> Dict[str, Any]:
        """处理单个文件，按原始步骤顺序逐个处理（执行或桥接），保证依赖连续性"""
        current_data: Dict[str, Any] = {"input": file_path}  # 初始输入（文件路径）
        results: Dict[str, Any] = {"original_path": file_path}  # 最终结果记录
        
        # 1. 判断文件类型（图片/视频）
        ext = os.path.splitext(file_path)[1].lower()
        is_image = ext in self.config.get("supported_image_formats", [])
        is_video = ext in self.config["supported_formats"]
        print(f"文件类型: {'图片' if is_image else '视频'}")
    
        # 2. 按原始步骤顺序逐个处理（关键修改：保持步骤顺序）
        for step in self.pipeline_steps:  # 遍历原始步骤列表，不提前拆分
            step_name = step["step_name"]
            output_key = step["output_key"]
            input_key = next(iter(step["input_params"].values()), None)  # 解析上游依赖
            
            # 检查上游依赖是否存在（此时上游步骤已处理，理论上必存在）
            if input_key is None:
                print(f"⚠️ 步骤 {step_name} 无输入参数，无法处理")
                continue
            if current_data.get(input_key) is None and results.get(input_key) is None:
                raise ValueError(f"步骤 {step_name} 依赖的 {input_key} 不存在（上游步骤未处理）")
    
            # 3. 判断是否需要跳过当前步骤
            if is_image and step_name.startswith("video"):
                # 跳过：桥接输出（用上游输入作为当前步骤的输出）
                bridge_value = current_data[input_key] or results[input_key]
                results[output_key] = bridge_value
                current_data[output_key] = bridge_value
                print(f"🔗 桥接跳过的步骤 {step_name}：{input_key} → {output_key}")
            else:
                # 执行：按原逻辑处理步骤
                module_name = step["module_name"]
                try:
                    if module_name not in self.modules:
                        raise ValueError(f"模块 {module_name} 未注册")
                    module_info = self.modules[module_name]
                    module_config = module_info["config"]
                    
                    # 准备参数（上游依赖已通过前面的检查，必存在）
                    params = {param_key: current_data[data_key] or results[data_key] 
                             for param_key, data_key in step["input_params"].items()}
                    
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
                    
                    # 更新数据
                    results[output_key] = result
                    current_data[output_key] = result
                    print(f"✅ 完成步骤 {step_name}（输出: {output_key}）")
                    
                except Exception as e:
                    print(f"❌ 步骤 {step_name} 失败: {str(e)}")
                    if self.config.get("stop_on_error", True):
                        raise
        
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

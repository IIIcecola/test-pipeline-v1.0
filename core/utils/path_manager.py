import os
import shutil
from typing import Dict, List, Tuple, Optional


class PathManager:
    def __init__(
        self,
        supported_image_formats: List[str] = None,
        supported_video_formats: List[str] = None,
        default_overwrite: bool = False
    ):
        """
        路径管理工具类，统一处理路径计算、目录创建、文件复制/移动及文件分类
        
        :param supported_image_formats: 支持的图片扩展名（带点，如 ['.jpg', '.png']）
        :param supported_video_formats: 支持的视频扩展名（带点，如 ['.mp4', '.avi']）
        :param default_overwrite: 默认覆盖策略（处理文件时是否覆盖已存在文件）
        """
        self.supported_image_formats = supported_image_formats or ['.jpg', '.jpeg', '.png', '.bmp']
        self.supported_video_formats = supported_video_formats or ['.mp4', '.avi', '.mov', '.mkv']
        self.default_overwrite = default_overwrite

    def get_relative_path(self, file_path: str, base_dir: str) -> str:
        """获取文件相对于基准目录的相对路径"""
        return os.path.relpath(file_path, base_dir)

    def ensure_output_dir(self, relative_path: str, output_root: str) -> Tuple[str, str]:
        """根据相对路径创建输出目录，返回输出目录和完整文件路径"""
        rel_dir = os.path.dirname(relative_path)
        file_name = os.path.basename(relative_path)
        output_dir = os.path.join(output_root, rel_dir)
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = os.path.join(output_dir, file_name)
        return output_dir, output_file_path

    def copy_file(
        self,
        source_file: str,
        source_root: str,
        output_root: str,
        overwrite: Optional[bool] = None
    ) -> str:
        """保持目录结构复制文件，返回目标路径"""
        overwrite = self.default_overwrite if overwrite is None else overwrite
        rel_path = self.get_relative_path(source_file, source_root)
        _, output_file = self.ensure_output_dir(rel_path, output_root)
        if not os.path.exists(output_file) or overwrite:
            shutil.copy2(source_file, output_file)  # 保留元数据
        return output_file

    def move_file(
        self,
        source_file: str,
        source_root: str,
        output_root: str,
        overwrite: Optional[bool] = None
    ) -> str:
        """保持目录结构移动文件，返回目标路径"""
        overwrite = self.default_overwrite if overwrite is None else overwrite
        rel_path = self.get_relative_path(source_file, source_root)
        _, output_file = self.ensure_output_dir(rel_path, output_root)
        if not os.path.exists(output_file) or overwrite:
            shutil.move(source_file, output_file)
        return output_file

    # 在现有PathManager类中添加以下方法
    def batch_copy_files(
        self,
        source_files: List[str],
        output_dir: str,
        overwrite: Optional[bool] = None
    ) -> List[str]:
        """
        批量复制文件到目标目录（不维持原始子目录，直接放在output_dir下）
        :return: 复制后的文件路径列表
        """
        overwrite = self.default_overwrite if overwrite is None else overwrite
        os.makedirs(output_dir, exist_ok=True)
        target_paths = []
        for source_file in source_files:
            file_name = os.path.basename(source_file)
            target_path = os.path.join(output_dir, file_name)
            if not os.path.exists(target_path) or overwrite:
                shutil.copy2(source_file, target_path)
            target_paths.append(target_path)
        return target_paths

    def batch_move_files(
        self,
        source_files: List[str],
        output_dir: str,
        overwrite: Optional[bool] = None
    ) -> List[str]:
        """批量移动文件到目标目录（不维持原始子目录）"""
        overwrite = self.default_overwrite if overwrite is None else overwrite
        os.makedirs(output_dir, exist_ok=True)
        target_paths = []
        for source_file in source_files:
            file_name = os.path.basename(source_file)
            target_path = os.path.join(output_dir, file_name)
            if not os.path.exists(target_path) or overwrite:
                shutil.move(source_file, target_path)
            target_paths.append(target_path)
        return target_paths

    def get_file_type(self, file_path: str) -> str:
        """判断文件类型（image/video/other）"""
        ext = os.path.splitext(file_path)[1].lower()
        if ext in self.supported_image_formats:
            return "image"
        elif ext in self.supported_video_formats:
            return "video"
        else:
            return "other"

    def classify_files(self, root_dir: str) -> Dict[str, List[str]]:
        """
        递归遍历目录，按文件类型分类
        
        :return: 分类结果，格式 {"image": [file_paths], "video": [file_paths], "other": [file_paths]}
        """
        classified = {"image": [], "video": [], "other": []}
        for root, _, files in os.walk(root_dir):
            for file in files:
                file_path = os.path.join(root, file)
                file_type = self.get_file_type(file_path)
                classified[file_type].append(file_path)
        return classified

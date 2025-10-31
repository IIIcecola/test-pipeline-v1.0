import os
import sys
from datetime import datetime
from pipeline_new import DataProcessingPipeline  # 导入管线类

# 设置默认路径（用户可根据实际情况修改）
DEFAULT_CONFIG_PATH = "configs/default_pipeline.json"
DEFAULT_INPUT_DIR = "data/input_videos"


def main(args: list = None) -> None:
    """
    主函数：解析命令行参数，执行视频处理管线
    
    参数:
        args: 命令行参数列表（默认为sys.argv[1:]）
    """
    # 处理命令行参数（默认使用sys.argv[1:]）
    if args is None:
        args = sys.argv[1:]
    
    # 解析参数：最多接收2个参数（config_path和input_dir）
    if len(args) > 2:
        print("❌ 错误：参数过多，格式应为：python pipeline_interface.py [配置文件路径] [输入目录路径]")
        return
    
    # 确定实际使用的路径（命令行参数优先，否则用默认值）
    config_path = args[0] if len(args) >= 1 else DEFAULT_CONFIG_PATH
    input_dir = args[1] if len(args) >= 2 else DEFAULT_INPUT_DIR
    
    # 验证路径存在性
    if not os.path.exists(config_path):
        print(f"❌ 错误：配置文件不存在 - {config_path}")
        return
    if not os.path.exists(input_dir):
        print(f"❌ 错误：输入目录不存在 - {input_dir}")
        return
    
    try:
        # 1. 创建管线并加载配置
        pipeline = DataProcessingPipeline(config_path)
        
        # 2. 打印管线信息
        print("=== 管线信息 ===")
        pipeline.list_modules()
        pipeline.list_steps()
        
        # 3. 运行管线处理视频
        print(f"\n=== 开始处理视频（输入目录: {input_dir}）===")
        results = pipeline.run(input_dir)
        
        if not results:
            print("⚠️ 未处理到任何视频文件")
            return
        
        # 4. 写入日志文件
        write_processing_log(
            results=results,
            pipeline_steps=pipeline.pipeline_steps
        )
        
        # 5. 打印处理统计
        print(f"\n=== 处理完成 ===")
        print(f"成功处理视频数: {len(results)}")
        print(f"视频列表: {list(results.keys())}")
        
    except Exception as e:
        print(f"❌ 处理过程出错: {str(e)}")
        sys.exit(1)  # 非0退出表示程序异常


if __name__ == "__main__":
    main()  # 调用主函数，自动处理命令行参数


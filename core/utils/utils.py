def write_processing_log(results: dict, pipeline_steps: list, log_dir: str = "processing_logs") -> None:
    """将批量处理结果写入日志文件（适配新的批量处理格式）"""
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(log_dir, f"processing_log_{timestamp}.txt")
    
    with open(log_file_path, "w", encoding="utf-8") as f:
        f.write(f"=== 数据处理管线日志 ===\n")
        f.write(f"处理时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"总步骤数: {len(pipeline_steps)}\n")
        f.write(f"步骤名称列表: {[step['step_name'] for step in pipeline_steps]}\n")
        f.write("="*50 + "\n\n")
        
        # 记录各步骤详细结果
        for step_idx, step_result in enumerate(results.get("steps", []), 1):
            f.write(f"【步骤{step_idx}】{step_result['step_name']}\n")
            f.write(f"输入目录: {step_result['input_dir']}\n")
            f.write(f"输出目录: {step_result['output_dir']}\n")
            f.write(f"输入文件分类: {step_result['input_classified']}\n")
            
            # 处理信息
            processed = step_result['step_result']
            f.write("处理统计:\n")
            f.write(f"  处理类型: {[t for t in processed['processed_types']]}\n")
            f.write(f"  处理文件数: {processed['processed_count']}\n")
            
            # 桥接信息
            if processed['bridged']:
                f.write("桥接信息:\n")
                for bridge in processed['bridged']:
                    f.write(f"  类型: {bridge['type']}, 数量: {bridge['count']}, 动作: {bridge['action']}\n")
            
            # 错误信息
            if processed['errors']:
                f.write("错误信息:\n")
                for error in processed['errors']:
                    f.write(f"  类型: {error['type']}, 阶段: {error['stage']}, 错误: {error['error']}\n")

            # 详细处理信息
            f.write(f"模块处理细节：\n")
            f.write(f"{processed["module_details"]}")
            f.write("-"*30 + "\n\n")
    
    print(f"\n✅ 日志文件已保存至: {log_file_path}")

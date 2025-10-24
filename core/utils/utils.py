def write_processing_log(results: dict, pipeline_steps: list, log_dir: str = "processing_logs") -> None:
    """将视频处理结果写入日志文件（保持原功能不变）"""
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(log_dir, f"video_processing_log_{timestamp}.txt")
    
    step_output_keys = [step["output_key"] for step in pipeline_steps]
    
    with open(log_file_path, "w", encoding="utf-8") as f:
        f.write(f"=== 视频处理管线日志 ===\n")
        f.write(f"处理时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"处理视频总数: {len(results)}\n")
        f.write(f"处理步骤数: {len(step_output_keys)}\n")
        f.write(f"步骤顺序及output_key: {dict(enumerate(step_output_keys, 1))}\n")
        f.write("="*50 + "\n\n")
        
        for idx, (filename, video_result) in enumerate(results.items(), 1):
            f.write(f"【{idx}】视频文件名: {filename}\n")
            f.write(f"原始文件路径: {video_result.get('original_path', '未知')}\n")
            f.write("各步骤处理结果:\n")
            
            for step_idx, output_key in enumerate(step_output_keys, 1):
                step_result = video_result.get(output_key, "未获取到结果")
                f.write(f"  步骤{step_idx}（output_key: {output_key}）: {step_result}\n")
            
            f.write("-"*30 + "\n\n")
    
    print(f"\n✅ 日志文件已保存至: {log_file_path}")

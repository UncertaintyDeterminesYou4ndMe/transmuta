use anyhow::{Result, anyhow};
use log::{info, warn};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

/// DiffOutputMode 定义了 diff 操作的输出模式
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DiffOutputMode {
    /// 并集：两个文件中所有的字段
    Union,
    /// 补集：在文件1或文件2中出现但不同时出现在两个文件中的字段
    Complement,
    /// 差集（以文件1为基准）：将文件2独有的字段添加到文件1
    DiffBasedOnFile1,
    /// 差集（以文件2为基准）：将文件1独有的字段添加到文件2
    DiffBasedOnFile2,
    /// 仅文件1有的字段（文件1 - 文件2）
    OnlyInFile1,
    /// 仅文件2有的字段（文件2 - 文件1）
    OnlyInFile2,
    /// 重新排序文件1的字段（不增减字段）
    SortFile1,
    /// 重新排序文件2的字段（不增减字段）
    SortFile2,
}

/// 用于字段比较的选项
#[derive(Debug, Clone)]
pub struct DiffOptions<'a> {
    /// 字段分隔符
    pub delimiter: char,
    /// 是否忽略大小写
    pub ignore_case: bool,
    /// 是否忽略空白字符
    pub ignore_whitespace: bool,
    /// 详细报告输出路径
    pub report_path: Option<&'a Path>,
}

/// 标准化字段名称，应用忽略选项
fn normalize_field(field: &str, options: &DiffOptions) -> String {
    let mut result = field.to_string();
    
    if options.ignore_whitespace {
        // 移除所有空白字符
        result = result.chars()
            .filter(|c| !c.is_whitespace())
            .collect();
    } else {
        // 仅修剪两端空白字符
        result = result.trim().to_string();
    }
    
    if options.ignore_case {
        // 转换为小写
        result = result.to_lowercase();
    }
    
    result
}

/// 从文件读取字段，返回排序后的字段集合
fn read_fields_from_file<'a>(file_path: &Path, options: &DiffOptions<'a>) -> Result<Vec<String>> {
    let file = File::open(file_path)
        .map_err(|e| anyhow!("无法打开文件 {}: {}", file_path.display(), e))?;
    
    let reader = BufReader::new(file);
    let mut fields = Vec::new();
    
    // 假设第一行是字段列表
    if let Some(line) = reader.lines().next() {
        let line = line.map_err(|e| anyhow!("读取文件 {} 首行失败: {}", file_path.display(), e))?;
        
        // 分割字段并应用标准化处理
        for field in line.split(options.delimiter) {
            let normalized = normalize_field(field, options);
            if !normalized.is_empty() {
                fields.push(normalized);
            }
        }
    } else {
        return Err(anyhow!("文件 {} 为空", file_path.display()));
    }
    
    // 排序字段以便双指针比较
    fields.sort();
    
    // 去重
    fields.dedup();
    
    Ok(fields)
}

/// 使用双指针算法计算差异
fn compute_diff(fields1: &[String], fields2: &[String]) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut union = Vec::new();
    let mut only_in_1 = Vec::new();
    let mut only_in_2 = Vec::new();
    
    let mut i = 0;
    let mut j = 0;
    
    while i < fields1.len() && j < fields2.len() {
        match fields1[i].cmp(&fields2[j]) {
            std::cmp::Ordering::Less => {
                // 字段在文件1中但不在文件2中
                union.push(fields1[i].clone());
                only_in_1.push(fields1[i].clone());
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                // 字段在文件2中但不在文件1中
                union.push(fields2[j].clone());
                only_in_2.push(fields2[j].clone());
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                // 字段在两个文件中都存在
                union.push(fields1[i].clone());
                i += 1;
                j += 1;
            }
        }
    }
    
    // 处理剩余的字段
    while i < fields1.len() {
        union.push(fields1[i].clone());
        only_in_1.push(fields1[i].clone());
        i += 1;
    }
    
    while j < fields2.len() {
        union.push(fields2[j].clone());
        only_in_2.push(fields2[j].clone());
        j += 1;
    }
    
    (union, only_in_1, only_in_2)
}

/// 将字段集合写入输出文件
fn write_fields_to_file(fields: &[String], output_path: &Path, delimiter: char) -> Result<()> {
    let mut file = File::create(output_path)
        .map_err(|e| anyhow!("无法创建输出文件 {}: {}", output_path.display(), e))?;
    
    let output = fields.join(&delimiter.to_string());
    file.write_all(output.as_bytes())
        .map_err(|e| anyhow!("写入输出文件 {} 失败: {}", output_path.display(), e))?;
    
    Ok(())
}

/// 生成详细的差异报告
fn generate_diff_report(
    input_file1: &Path, 
    input_file2: &Path,
    original_fields1: &[String],
    original_fields2: &[String],
    only_in_1: &[String], 
    only_in_2: &[String],
    common_count: usize,
    output_path: &Path,
) -> Result<()> {
    let mut file = File::create(output_path)
        .map_err(|e| anyhow!("无法创建报告文件 {}: {}", output_path.display(), e))?;
    
    // 编写报告标题
    writeln!(file, "字段差异比较报告")?;
    writeln!(file, "==================")?;
    writeln!(file, "")?;
    
    // 文件信息
    writeln!(file, "文件1: {}", input_file1.display())?;
    writeln!(file, "文件2: {}", input_file2.display())?;
    writeln!(file, "")?;
    
    // 差异统计
    writeln!(file, "差异统计")?;
    writeln!(file, "--------")?;
    writeln!(file, "文件1字段数: {}", original_fields1.len())?;
    writeln!(file, "文件2字段数: {}", original_fields2.len())?;
    writeln!(file, "两个文件共有字段数: {}", common_count)?;
    writeln!(file, "仅在文件1中的字段数: {}", only_in_1.len())?;
    writeln!(file, "仅在文件2中的字段数: {}", only_in_2.len())?;
    writeln!(file, "")?;
    
    // 仅在文件1中的字段
    writeln!(file, "仅在文件1中的字段")?;
    writeln!(file, "----------------")?;
    for field in only_in_1 {
        writeln!(file, "- {}", field)?;
    }
    if only_in_1.is_empty() {
        writeln!(file, "(无)")?;
    }
    writeln!(file, "")?;
    
    // 仅在文件2中的字段
    writeln!(file, "仅在文件2中的字段")?;
    writeln!(file, "----------------")?;
    for field in only_in_2 {
        writeln!(file, "- {}", field)?;
    }
    if only_in_2.is_empty() {
        writeln!(file, "(无)")?;
    }
    writeln!(file, "")?;
    
    // 两个文件的完整字段列表
    writeln!(file, "文件1的完整字段列表")?;
    writeln!(file, "------------------")?;
    for field in original_fields1 {
        writeln!(file, "- {}", field)?;
    }
    writeln!(file, "")?;
    
    writeln!(file, "文件2的完整字段列表")?;
    writeln!(file, "------------------")?;
    for field in original_fields2 {
        writeln!(file, "- {}", field)?;
    }
    
    info!("详细的差异报告已写入: {}", output_path.display());
    
    Ok(())
}

/// 执行字段差异比较并生成输出
pub fn diff_fields<'a>(
    input_file1: &Path,
    input_file2: &Path,
    output_path: &Path,
    mode: DiffOutputMode,
    options: DiffOptions<'a>,
) -> Result<()> {
    info!("正在比较文件 {} 和 {} 的字段差异", input_file1.display(), input_file2.display());
    
    // 读取两个文件的字段
    let fields1 = read_fields_from_file(input_file1, &options)?;
    let fields2 = read_fields_from_file(input_file2, &options)?;
    
    // 保存原始排序的字段列表（用于报告）
    let original_fields1 = fields1.clone();
    let original_fields2 = fields2.clone();
    
    info!("文件1字段数: {}", fields1.len());
    info!("文件2字段数: {}", fields2.len());
    
    // 计算差异
    let (union, only_in_1, only_in_2) = compute_diff(&fields1, &fields2);
    
    // 计算共有字段数
    let common_count = fields1.len() + fields2.len() - union.len();
    
    info!("两个文件字段并集数: {}", union.len());
    info!("两个文件共有字段数: {}", common_count);
    info!("仅在文件1中的字段数: {}", only_in_1.len());
    info!("仅在文件2中的字段数: {}", only_in_2.len());
    
    // 根据输出模式生成结果
    let output_fields = match mode {
        DiffOutputMode::Union => union,
        DiffOutputMode::Complement => {
            let mut complement = Vec::new();
            complement.extend_from_slice(&only_in_1);
            complement.extend_from_slice(&only_in_2);
            complement.sort();
            complement
        },
        DiffOutputMode::DiffBasedOnFile1 => {
            let mut result = fields1.clone();
            result.extend_from_slice(&only_in_2);
            result.sort();
            result
        },
        DiffOutputMode::DiffBasedOnFile2 => {
            let mut result = fields2.clone();
            result.extend_from_slice(&only_in_1);
            result.sort();
            result
        },
        DiffOutputMode::OnlyInFile1 => only_in_1.clone(),
        DiffOutputMode::OnlyInFile2 => only_in_2.clone(),
        DiffOutputMode::SortFile1 => fields1.clone(),
        DiffOutputMode::SortFile2 => fields2.clone(),
    };
    
    // 写入输出文件
    write_fields_to_file(&output_fields, output_path, options.delimiter)?;
    
    // 如果需要，生成详细报告
    if let Some(report_path) = options.report_path {
        generate_diff_report(
            input_file1, 
            input_file2,
            &original_fields1,
            &original_fields2,
            &only_in_1, 
            &only_in_2,
            common_count,
            report_path
        )?;
    }
    
    info!("字段差异比较完成，结果已写入 {}", output_path.display());
    info!("输出字段数: {}", output_fields.len());
    
    Ok(())
} 
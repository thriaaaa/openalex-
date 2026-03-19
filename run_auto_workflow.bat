@echo off
chcp 65001 >nul
echo.
echo ╔══════════════════════════════════════════════════════════╗
echo ║     OpenAlex 全自动化数据处理流程                         ║
echo ║                                                          ║
echo ║  功能：下载 → 展平 → 去重合并 → 模式映射                 ║
echo ╚══════════════════════════════════════════════════════════╝
echo.
echo [配置] 请确认已修改 openalex_auto_config.json
echo.
echo  步骤说明:
echo    download        从 OpenAlex S3 下载增量数据
echo    flatten         展平 JSONL 为 CSV
echo    merge           去重合并
echo    schema_mapping  模式映射（顶点/文档/边/向量）
echo.
echo  运行模式（请选择）:
echo    [1] 全流程执行（按配置文件中 workflow.steps）
echo    [2] 只执行模式映射（schema_mapping）
echo    [3] 只执行下载+展平+合并（不含模式映射）
echo    [4] 自定义步骤（命令行参数模式）
echo    [0] 退出
echo.
set /p CHOICE=请输入选项编号 [0-4]: 

if "%CHOICE%"=="0" goto :EOF

if "%CHOICE%"=="1" (
    echo.
    echo [开始] 全流程执行...
    python openalex_auto_workflow.py
    goto :DONE
)

if "%CHOICE%"=="2" (
    echo.
    echo [开始] 只执行 schema_mapping...
    echo  (如需只执行部分子步骤，请用选项4或直接运行 python openalex_auto_workflow.py --steps schema_mapping --mapping-steps 01 02)
    python openalex_auto_workflow.py --steps schema_mapping
    goto :DONE
)

if "%CHOICE%"=="3" (
    echo.
    echo [开始] 执行 下载+展平+合并...
    python openalex_auto_workflow.py --steps download flatten merge
    goto :DONE
)

if "%CHOICE%"=="4" (
    echo.
    echo 示例:
    echo   python openalex_auto_workflow.py --steps schema_mapping --mapping-steps 01 02 03
    echo   python openalex_auto_workflow.py --steps merge schema_mapping
    echo   python openalex_auto_workflow.py --help
    echo.
    set /p CUSTOM_CMD=请输入完整命令参数 (python openalex_auto_workflow.py 之后的部分): 
    python openalex_auto_workflow.py %CUSTOM_CMD%
    goto :DONE
)

echo [错误] 无效选项: %CHOICE%
goto :EOF

:DONE
echo.
echo [完成] 流程执行完毕
echo [日志] 查看 openalex_auto_workflow.log 与 schema_mapping\schema_mapping.log 了解详情
echo.
pause

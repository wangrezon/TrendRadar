# coding=utf-8
"""
AI 客户端模块

基于 LiteLLM 的统一 AI 模型接口
支持 100+ AI 提供商（OpenAI、DeepSeek、Gemini、Claude、国内模型等）
"""

import json
import os
from typing import Any, Callable, Dict, List, Optional

import litellm
from litellm import completion


class AIClient:
    """统一的 AI 客户端（基于 LiteLLM）"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化 AI 客户端

        Args:
            config: AI 配置字典
                - MODEL: 模型标识（格式: provider/model_name）
                - API_KEY: API 密钥
                - API_BASE: API 基础 URL（可选）
                - TEMPERATURE: 采样温度
                - MAX_TOKENS: 最大生成 token 数
                - TIMEOUT: 请求超时时间（秒）
                - NUM_RETRIES: 重试次数（可选）
                - FALLBACK_MODELS: 备用模型列表（可选）
        """
        self.model = config.get("MODEL", "deepseek/deepseek-chat")
        self.api_key = config.get("API_KEY") or os.environ.get("AI_API_KEY", "")
        self.api_base = config.get("API_BASE", "")
        self.temperature = config.get("TEMPERATURE", 1.0)
        self.max_tokens = config.get("MAX_TOKENS", 5000)
        self.timeout = config.get("TIMEOUT", 120)
        self.num_retries = config.get("NUM_RETRIES", 2)
        self.fallback_models = config.get("FALLBACK_MODELS", [])

    def chat(
        self,
        messages: List[Dict[str, str]],
        **kwargs
    ) -> str:
        """
        调用 AI 模型进行对话

        Args:
            messages: 消息列表，格式: [{"role": "system/user/assistant", "content": "..."}]
            **kwargs: 额外参数，会覆盖默认配置

        Returns:
            str: AI 响应内容

        Raises:
            Exception: API 调用失败时抛出异常
        """
        params = self._build_params(messages, **kwargs)

        # 调用 LiteLLM
        response = completion(**params)

        # 提取响应内容
        return response.choices[0].message.content

    def _build_params(self, messages: List[Dict], **kwargs) -> Dict[str, Any]:
        """构建 LiteLLM completion 请求参数（chat 和 chat_with_tools 共用）。"""
        params = {
            "model": self.model,
            "messages": messages,
            "temperature": kwargs.get("temperature", self.temperature),
            "timeout": kwargs.get("timeout", self.timeout),
            "num_retries": kwargs.get("num_retries", self.num_retries),
        }

        if self.api_key:
            params["api_key"] = self.api_key

        if self.api_base:
            params["api_base"] = self.api_base

        max_tokens = kwargs.get("max_tokens", self.max_tokens)
        if max_tokens and max_tokens > 0:
            params["max_tokens"] = max_tokens

        if self.fallback_models:
            params["fallbacks"] = self.fallback_models

        # 合并其他额外参数
        for key, value in kwargs.items():
            if key not in params:
                params[key] = value

        return params

    def chat_with_tools(
        self,
        messages: List[Dict],
        tools: List[Dict],
        tool_executor: Callable[[str, Dict], str],
        max_rounds: int = 30,
        **kwargs
    ) -> str:
        """
        带工具调用的对话（Function Calling / Tool Use）。

        模型可在生成过程中请求调用工具，客户端执行工具后将结果反馈给模型，
        循环直到模型返回最终文本或达到最大轮数。

        若当前模型不支持 function calling，自动降级为普通 chat()。

        Args:
            messages: 消息列表
            tools: 工具 JSON Schema 列表（OpenAI tools 格式）
            tool_executor: 工具执行回调，签名 (function_name, arguments_dict) -> str
            max_rounds: 最大工具调用轮数（防止无限循环）
            **kwargs: 额外参数，会覆盖默认配置

        Returns:
            str: AI 最终响应内容
        """
        # 检测模型是否支持 function calling
        try:
            if not litellm.supports_function_calling(model=self.model):
                print(f"[AI] 模型 {self.model} 不支持 Function Calling，降级为普通对话")
                return self.chat(messages, **kwargs)
        except Exception:
            # supports_function_calling 可能对未知模型抛异常，安全降级
            print(f"[AI] 无法检测模型 {self.model} 的 Function Calling 支持情况，尝试继续")

        # 复制消息列表，避免修改原始数据
        messages = list(messages)

        tool_call_seq = 0  # 全局工具调用计数器

        for round_idx in range(max_rounds):
            params = self._build_params(messages, **kwargs)
            params["tools"] = tools
            params["tool_choice"] = "auto"

            response = completion(**params)
            response_message = response.choices[0].message

            # 如果模型没有请求工具调用，返回最终内容
            if not response_message.tool_calls:
                content = response_message.content or ""
                print(f"[AI] 模型响应完成（第 {round_idx + 1} 轮，共 {tool_call_seq} 次工具调用，{len(content)} 字符）")
                return content

            # 模型请求了工具调用 —— 追加 assistant 消息
            messages.append(response_message)

            # 逐个执行工具调用
            for tool_call in response_message.tool_calls:
                func_name = tool_call.function.name
                try:
                    func_args = json.loads(tool_call.function.arguments)
                except (json.JSONDecodeError, TypeError):
                    func_args = {}

                tool_call_seq += 1
                print(f"[AI] 工具调用 #{tool_call_seq}: {func_name}({func_args})")

                # 执行工具
                tool_result = tool_executor(func_name, func_args)

                # 打印工具返回结果（截断避免日志过长）
                result_preview = tool_result[:200] if len(tool_result) > 200 else tool_result
                print(f"[AI] 工具结果 #{tool_call_seq} ({func_name}): "
                      f"[{len(tool_result)} 字符]\n{result_preview}")
                if len(tool_result) > 200:
                    print(f"[AI] ... 结果已截断，完整长度 {len(tool_result)} 字符")

                # 追加工具结果消息
                messages.append({
                    "tool_call_id": tool_call.id,
                    "role": "tool",
                    "name": func_name,
                    "content": tool_result,
                })

        # 达到最大轮数后，做一次不带 tools 的请求以获取最终回答
        print(f"[AI] 已达最大工具调用轮数 ({max_rounds})，请求最终回答")
        params = self._build_params(messages, **kwargs)
        response = completion(**params)
        content = response.choices[0].message.content or ""
        print(f"[AI] 模型最终响应完成（{len(content)} 字符）")
        return content

    def validate_config(self) -> tuple[bool, str]:
        """
        验证配置是否有效

        Returns:
            tuple: (是否有效, 错误信息)
        """
        if not self.model:
            return False, "未配置 AI 模型（model）"

        if not self.api_key:
            return False, "未配置 AI API Key，请在 config.yaml 或环境变量 AI_API_KEY 中设置"

        # 验证模型格式（应该包含 provider/model）
        if "/" not in self.model:
            return False, f"模型格式错误: {self.model}，应为 'provider/model' 格式（如 'deepseek/deepseek-chat'）"

        return True, ""

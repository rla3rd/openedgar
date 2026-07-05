import os
import requests
import json
import time
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class InferenceProvider:
    def __init__(self, provider_type="openai", model="qwen3-coder-30b", api_url=None, api_key=None):
        self.provider_type = provider_type.lower()
        self.model = model
        self.api_url = api_url or os.getenv("INFERENCE_API_URL", "http://localhost:1234/v1/chat/completions")
        self.api_key = api_key or os.getenv("INFERENCE_API_KEY", "no-key-required")

    def call(self, prompt: str, system_prompt: str = "You are a helpful assistant.", temperature: float = 0.0, max_tokens: int = 8192) -> str:
        if self.provider_type in ["openai", "local", "vllm", "lm-studio", "groq"]:
            return self._call_openai_compatible(prompt, system_prompt, temperature, max_tokens)
        elif self.provider_type == "anthropic":
            return self._call_anthropic(prompt, system_prompt, temperature, max_tokens)
        elif self.provider_type == "google":
            return self._call_google(prompt, system_prompt, temperature, max_tokens)
        elif self.provider_type == "huggingface":
            return self._call_huggingface(prompt, system_prompt, temperature, max_tokens)
        else:
            raise ValueError(f"Unknown provider type: {self.provider_type}")

    def _call_openai_compatible(self, prompt, system_prompt, temperature, max_tokens):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
        max_retries = 5
        base_delay = 2
        
        for attempt in range(max_retries):
            try:
                response = requests.post(self.api_url, headers=headers, json=payload, timeout=600)
                if response.status_code != 200:
                    print(f"DEBUG: Inference error {response.status_code}: {response.text}")
                response.raise_for_status()
                return response.json()["choices"][0]["message"]["content"]
            except (requests.exceptions.RequestException, KeyError) as e:
                if attempt == max_retries - 1:
                    raise
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Inference error: {e}. Retrying in {delay}s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(delay)
        
        raise RuntimeError("Max retries exceeded for inference.")

    def _call_anthropic(self, prompt, system_prompt, temperature, max_tokens):
        """Call Anthropic Claude via the Messages API."""
        headers = {
            'x-api-key': self.api_key,
            'anthropic-version': '2023-06-01',
            'content-type': 'application/json',
        }
        payload = {
            'model': self.model,
            'max_tokens': max_tokens,
            'system': system_prompt,
            'messages': [{'role': 'user', 'content': prompt}],
            'temperature': temperature,
        }
        response = requests.post(
            self.api_url or 'https://api.anthropic.com/v1/messages',
            headers=headers, json=payload, timeout=600
        )
        response.raise_for_status()
        return response.json()['content'][0]['text']

    def _call_google(self, prompt, system_prompt, temperature, max_tokens):
        """Call Google Gemini via the google-genai SDK."""
        from google import genai
        from google.genai import types
        # Use the key passed in at construction (from user profile), not env var
        client = genai.Client(api_key=self.api_key)
        response = client.models.generate_content(
            model=self.model,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=temperature,
                max_output_tokens=max_tokens,
            ),
        )
        return response.text

    def _call_huggingface(self, prompt, system_prompt, temperature, max_tokens):
        # Implementation for Hugging Face API
        # model: qwen3-vl-235b-instruct, meta-llama/Meta-Llama-3-70B-Instruct
        from huggingface_hub import InferenceClient
        self.api_key = os.getenv("HF_TOKEN")
        client = InferenceClient(api_key=self.api_key)
        
        completion = client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return completion.choices[0].message.content
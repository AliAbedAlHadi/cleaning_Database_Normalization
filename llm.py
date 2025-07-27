import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
API_URL = os.getenv("API_URL")
MODEL_NAME = os.getenv("MODEL_NAME")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

def call_llm(prompt: str) -> str:
    """
    Sends a prompt to a Chat-completions-compatible API and returns the response text.
    """
    if not API_KEY or not API_URL or not MODEL_NAME:
        raise ValueError("API_KEY, API_URL, and MODEL_NAME must be set in environment variables.")

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": "You are a helpful SQL assistant."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 512,
        "temperature": 0.7
    }

    try:
        response = requests.post(API_URL, headers=HEADERS, json=payload)
        response.raise_for_status()
        data = response.json()

        # Adapted for OpenAI-style API:
        return data["choices"][0]["message"]["content"].strip()

    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"LLM API request failed: {e}")
    except (ValueError, KeyError, IndexError) as e:
        raise RuntimeError(f"Unexpected response format: {e}")

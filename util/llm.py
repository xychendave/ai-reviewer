import json
import re

import backoff
import openai
from zhipuai import ZhipuAI

zpai_client = ZhipuAI(api_key="45336a2d0f934cde8e11c80f726e4ab9.eIKUaQn3KGOKjq88") 


MAX_NUM_TOKENS = 4096

# Get N responses from a single message, used for ensembling.
@backoff.on_exception(backoff.expo, (openai.RateLimitError, openai.APITimeoutError))
def get_batch_responses_from_llm(
        msg,
        client,
        model,
        system_message,
        msg_history=None,
        temperature=0.75,
        n_responses=1,
):
    if msg_history is None:
        msg_history = []

    content, new_msg_history = [], []
    for _ in range(n_responses):
        c, hist = get_response_from_llm(
            msg,
            client,
            model,
            system_message,
            msg_history=None,
            temperature=temperature,
        )
        print(c, hist)
        content.append(c)
        new_msg_history.append(hist)

    return content, new_msg_history


@backoff.on_exception(backoff.expo, (openai.RateLimitError, openai.APITimeoutError))
def get_response_from_llm(
        msg,
        client,
        model,
        system_message,
        msg_history=None,
        temperature=0.75,
):
    if msg_history is None:
        msg_history = []

    new_msg_history = msg_history + [{"role": "user", "content": msg}]
    response = zpai_client.chat.completions.create(
        model="glm-4-plus",
        messages=[
            {"role": "system", "content": system_message},
            *new_msg_history,
        ],
        temperature=temperature,
        max_tokens=MAX_NUM_TOKENS
    )
    content = response.choices[0].message.content
    new_msg_history = new_msg_history + [{"role": "assistant", "content": content}]

    return content, new_msg_history


def extract_json_between_markers(llm_output):
    # Regular expression pattern to find JSON content between ```json and ```
    json_pattern = r"```json(.*?)```"
    matches = re.findall(json_pattern, llm_output, re.DOTALL)

    if not matches:
        # Fallback: Try to find any JSON-like content in the output
        json_pattern = r"\{.*?\}"
        matches = re.findall(json_pattern, llm_output, re.DOTALL)

    for json_string in matches:
        json_string = json_string.strip()
        try:
            parsed_json = json.loads(json_string)
            return parsed_json
        except json.JSONDecodeError:
            # Attempt to fix common JSON issues
            try:
                # Remove invalid control characters
                json_string_clean = re.sub(r"[\x00-\x1F\x7F]", "", json_string)
                parsed_json = json.loads(json_string_clean)
                return parsed_json
            except json.JSONDecodeError:
                continue  # Try next match

    return None  # No valid JSON found


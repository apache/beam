#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import logging
import pickle
import sys
import uvicorn
from fastapi import FastAPI, Response, Request, HTTPException
from pydantic import BaseModel
from typing import List, Any

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("subprocess_server")

app = FastAPI()
handler = None
model = None

def _extract_inference(result) -> str:
  if hasattr(result, "inference"):
    val = result.inference
  elif hasattr(result, "text"):
    val = result.text
  else:
    val = result
  return str(val)

class ChatMessage(BaseModel):
  role: str
  content: str

class ChatCompletionRequest(BaseModel):
  model: str
  messages: List[ChatMessage]
  temperature: float = 1.0
  max_tokens: int = 1024

class CompletionRequest(BaseModel):
  model: str
  prompt: str

@app.post("/v1/chat/completions")
async def chat_completions(request: ChatCompletionRequest):
  logger.info("Received chat completion request")
  # Map to handler input.
  # For compatibility with standard text handlers, we take the last user message content.
  # If the handler expects the full history, we might need a more complex mapping.
  prompt = request.messages[-1].content
  
  # Run inference
  # handler.run_inference expects a sequence of inputs
  try:
    results = handler.run_inference([prompt], model)
    result = list(results)[0]
    inference_text = _extract_inference(result)
  except Exception as e:
    logger.exception("Error during inference")
    raise HTTPException(status_code=500, detail=str(e))
  
  return {
      "choices": [{
          "message": {
              "role": "assistant",
              "content": inference_text
          }
      }]
  }

@app.post("/v1/completions")
async def completions(request: CompletionRequest):
  logger.info("Received completion request")
  try:
    results = handler.run_inference([request.prompt], model)
    result = list(results)[0]
    inference_text = _extract_inference(result)
  except Exception as e:
    logger.exception("Error during inference")
    raise HTTPException(status_code=500, detail=str(e))
    
  return {
      "choices": [{
          "text": inference_text
      }]
  }

@app.post("/v1/beam/inference")
async def beam_inference(request: Request):
  logger.info("Received Beam raw inference request")
  try:
    body = await request.body()
    payload = pickle.loads(body)
    batch = payload["batch"]
    inference_args = payload.get("inference_args")
    
    results = handler.run_inference(batch, model, inference_args)
    results_list = list(results)
    pickled_results = pickle.dumps(results_list)
    return Response(content=pickled_results, media_type="application/octet-stream")
  except Exception as e:
    logger.exception("Error during raw inference")
    raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/models")
async def list_models():
  # Endpoint to check connectivity and list models
  model_name = getattr(handler, "_model_name", "unknown")
  return {
      "data": [
          {
              "id": model_name,
              "object": "model",
          }
      ]
  }

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--handler_path", required=True)
  parser.add_argument("--port", type=int, required=True)
  args = parser.parse_args()
  
  logger.info("Loading handler from %s", args.handler_path)
  with open(args.handler_path, "rb") as f:
    handler = pickle.load(f)
    
  logger.info("Loading model...")
  model = handler.load_model()
  logger.info("Starting uvicorn server on port %d", args.port)
  uvicorn.run(app, host="127.0.0.1", port=args.port)

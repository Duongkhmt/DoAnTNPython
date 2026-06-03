import os
import uvicorn
import queue
import threading
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from langchain_core.callbacks import BaseCallbackHandler
from chatbot_agent import chat_with_agent, agent_executor

app = FastAPI(title="Smart Finance Chatbot API")

# Cấu hình CORS cho phép React frontend gọi trực tiếp
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatRequest(BaseModel):
    message: str

class ChatResponse(BaseModel):
    response: str

class UnifiedStreamingParser:
    def __init__(self, token_callback):
        self.token_callback = token_callback
        self.mode = "UNKNOWN"  # UNKNOWN, RAW, JSON
        self.buffer = ""
        
        # JSON parser state
        self.in_action_input = False
        self.escaped = False
        self.action = None
        self.is_final_answer = False

    def process_token(self, token: str):
        # Nếu thấy ký tự bắt đầu code block mới (như ```json hoặc ```), tự động reset parser
        if "```json" in token or "```" in token:
            if self.mode == "JSON":
                if not self.in_action_input:
                    self.mode = "UNKNOWN"
                    self.buffer = ""
                    self.action = None
                    self.is_final_answer = False
                    self.in_action_input = False
                    self.escaped = False
                    return

        if self.mode == "RAW":
            self.token_callback(token)
            return

        if self.mode == "UNKNOWN":
            self.buffer += token
            # Decide mode khi có đủ text hoặc dòng mới
            if len(self.buffer) >= 15 or "\n" in self.buffer:
                cleaned = self.buffer.strip()
                if cleaned.startswith("{") or cleaned.startswith("```") or "action" in cleaned:
                    self.mode = "JSON"
                    self.process_json_buffer()
                else:
                    self.mode = "RAW"
                    self.token_callback(self.buffer)
                    self.buffer = ""
            return

        if self.mode == "JSON":
            if not self.in_action_input:
                self.buffer += token
                self.process_json_buffer()
            else:
                self.process_content(token)

    def process_json_buffer(self):
        import sys
        import re
        
        sys.stderr.write(f"[DEBUG] PARSER STATE: action={self.action}, is_final_answer={self.is_final_answer}, in_action_input={self.in_action_input}, buffer_len={len(self.buffer)}\n")
        sys.stderr.flush()
        
        # 1. Tìm action sử dụng regex để tăng độ chính xác và tránh bị lệch token boundary
        if not self.action:
            m_action = re.search(r'"action"\s*:\s*"([^"]+)"', self.buffer)
            if m_action:
                self.action = m_action.group(1)
                sys.stderr.write(f"[DEBUG] FOUND ACTION: {self.action}\n")
                sys.stderr.flush()
                if self.action == "Final Answer":
                    self.is_final_answer = True

        # 2. Tìm điểm bắt đầu của action_input sử dụng regex
        if self.is_final_answer and not self.in_action_input:
            m_input = re.search(r'"action_input"\s*:\s*"', self.buffer)
            if m_input:
                self.in_action_input = True
                start_idx = m_input.end()
                content_start = self.buffer[start_idx:]
                sys.stderr.write(f"[DEBUG] DETECTED ACTION_INPUT START, content_start={repr(content_start)}\n")
                sys.stderr.flush()
                self.buffer = ""  # Xóa buffer để bắt đầu stream văn bản
                if content_start:
                    self.process_content(content_start)

    def process_content(self, text: str):
        import sys
        for char in text:
            if self.escaped:
                if char == 'n':
                    self.token_callback('\n')
                elif char == 't':
                    self.token_callback('\t')
                elif char == 'r':
                    self.token_callback('\r')
                elif char == '"':
                    self.token_callback('"')
                elif char == '\\':
                    self.token_callback('\\')
                else:
                    self.token_callback('\\' + char)
                self.escaped = False
            elif char == '\\':
                self.escaped = True
            elif char == '"':
                sys.stderr.write("[DEBUG] DETECTED CLOSING QUOTE\n")
                sys.stderr.flush()
                self.in_action_input = False
                self.is_final_answer = False
            else:
                self.token_callback(char)

    def end_of_stream(self):
        if self.mode == "UNKNOWN" and self.buffer:
            self.token_callback(self.buffer)
        self.buffer = ""

@app.post("/api/chat", response_model=ChatResponse)
async def chat_endpoint(req: ChatRequest):
    if not req.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    try:
        # Gọi agent xử lý câu hỏi
        result = chat_with_agent(req.message)
        return ChatResponse(response=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/chat/stream")
async def chat_stream_endpoint(req: ChatRequest):
    if not req.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    def event_generator():
        q = queue.Queue()
        import sys
        
        def token_callback(token):
            sys.stderr.write(f"[DEBUG] EMITTING TOKEN: {repr(token)}\n")
            sys.stderr.flush()
            q.put(token)
            
        parser = UnifiedStreamingParser(token_callback)
        
        class StreamingAgentHandler(BaseCallbackHandler):
            def on_llm_start(self, serialized, prompts, **kwargs):
                sys.stderr.write("[DEBUG] LLM START\n")
                sys.stderr.flush()
                parser.mode = "UNKNOWN"
                parser.buffer = ""
                parser.action = None
                parser.is_final_answer = False
                parser.in_action_input = False
                parser.escaped = False
                
            def on_llm_new_token(self, token: str, **kwargs):
                sys.stderr.write(f"[DEBUG] NEW TOKEN: {repr(token)}\n")
                sys.stderr.flush()
                parser.process_token(token)
                
            def on_llm_end(self, response, **kwargs):
                pass
                
        handler = StreamingAgentHandler()
        
        def run_agent():
            try:
                # Gọi agent executor với callback handler để stream tokens
                agent_executor.invoke({"input": req.message}, {"callbacks": [handler]})
            except Exception as e:
                q.put(f"[ERROR] Gặp lỗi khi xử lý câu hỏi: {str(e)}")
            finally:
                parser.end_of_stream()
                q.put(None)  # Sentinel to signal end of queue
                
        thread = threading.Thread(target=run_agent)
        thread.start()
        
        while True:
            token = q.get()
            if token is None:
                break
            yield token

    return StreamingResponse(event_generator(), media_type="text/plain")

if __name__ == "__main__":
    # Sử dụng port 8090 để tránh trùng với Spring Boot (8082) và React (5173)
    uvicorn.run(app, host="0.0.0.0", port=8090)

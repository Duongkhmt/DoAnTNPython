# Biểu Đồ Tuần Tự Hệ Thống Smart Finance Chatbot

Tài liệu này cung cấp biểu đồ tuần tự (Sequence Diagram) chi tiết mô tả luồng đi của dữ liệu từ khi người dùng nhập câu hỏi trên giao diện Dashboard cho đến khi nhận được phản hồi từ AI.

```mermaid
sequenceDiagram
    autonumber
    actor User as Nhà đầu tư
    participant React as React Frontend (ChatWidget.jsx)
    participant LS as Browser LocalStorage
    participant FastAPI as FastAPI Server (chatbot_api.py:8089)
    participant Agent as LangChain Agent (chatbot_agent.py)
    participant Tools as Python Tools (chatbot_tools.py)
    participant DB as PostgreSQL / TimescaleDB
    participant DeepSeek as DeepSeek-v4-Pro (API Box)

    User->>React: Nhập câu hỏi (Ví dụ: "Phân tích mã SHB")
    React->>React: Cập nhật trạng thái tin nhắn mới & Hiển thị bong bóng chờ (Thinking bubble)
    React->>FastAPI: Gửi HTTP POST /api/chat {"message": "Phân tích mã SHB"}
    
    FastAPI->>Agent: Gọi hàm chat_with_agent("Phân tích mã SHB")
    Agent->>Agent: Tải lịch sử hội thoại từ memory (ConversationBufferMemory)
    Agent->>DeepSeek: Gửi Prompt hệ thống + Tin nhắn + Danh sách Tools khả dụng
    
    Note over DeepSeek: Lập luận ý định (Intent Analysis)<br/>Nhận diện cần thông tin phân tích cổ phiếu SHB
    DeepSeek-->>Agent: Trả về Action: Gọi tool "Get_Full_Stock_Analysis_Data" với Input: "SHB"
    
    Agent->>Tools: Gọi hàm get_full_stock_analysis("SHB")
    
    rect rgb(240, 248, 255)
        Note over Tools, DB: Thực thi các câu truy vấn cơ sở dữ liệu đồng thời
        Tools->>DB: 1. Lấy giá, SMA20, RSI, Khối lượng giao dịch gần nhất
        DB-->>Tools: Trả về số liệu giao dịch
        Tools->>DB: 2. Lấy điểm dự báo AI Score & tín hiệu khuyến nghị
        DB-->>Tools: Trả về kết quả phân tích học máy
        Tools->>DB: 3. Lấy pha Wyckoff & tín hiệu VSA (Cung cạn, Stopping Vol...)
        DB-->>Tools: Trả về cấu trúc tích lũy/phân phối
        Tools->>DB: 4. Lấy danh sách ngành & Tính toán sức mạnh so với trung bình ngành
        DB-->>Tools: Trả về dữ liệu ngành
    end

    Tools-->>Agent: Trả về chuỗi dữ liệu gộp thô (Observation)
    
    Agent->>DeepSeek: Gửi dữ liệu thô (Observation) từ Tool về cho LLM
    Note over DeepSeek: Tổng hợp dữ liệu thô,<br/>áp dụng tư duy phân tích tài chính,<br/>viết báo cáo phân tích bằng Tiếng Việt định dạng Markdown
    DeepSeek-->>Agent: Trả về văn bản phản hồi hoàn chỉnh
    
    Agent-->>FastAPI: Trả về chuỗi văn bản (Response Text)
    FastAPI-->>React: Trả về HTTP 200 JSON {"response": "..."}
    
    React->>LS: Lưu tin nhắn mới vào localStorage ('ai_chat_history')
    
    loop Hiệu ứng Typewriter
        React->>User: Hiển thị chữ chạy lần lượt (3 ký tự mỗi 10ms) & tự động cuộn màn hình
    end
```

## Các điểm lưu ý trong biểu đồ tuần tự:
1.  **Bước 5 & 6:** Agent sử dụng cơ chế ReAct để tự động nhận dạng công cụ thích hợp. Nhờ vào việc sử dụng mô hình lập luận cao cấp `deepseek-v4-pro`, Agent có khả năng phân tích ý định rất chuẩn xác.
2.  **Bước 8 đến 12 (Database block):** Toàn bộ việc truy vấn database được gộp chung trong 1 tool duy nhất để giảm thiểu số lượng API round-trip gửi sang DeepSeek. Điều này tối ưu thời gian phản hồi từ ~15 giây xuống còn ~3.5 giây.
3.  **Bước 18 (Typewriter effect):** Thay vì hiển thị toàn bộ nội dung lớn ngay lập tức gây ngột ngạt cho người dùng, Frontend sử dụng vòng lặp thời gian để hiển thị nội dung trôi chảy như đang được stream.

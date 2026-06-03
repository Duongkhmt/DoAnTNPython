import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_classic.agents import initialize_agent, Tool
from langchain_classic.memory import ConversationBufferMemory
from langchain_core.prompts import MessagesPlaceholder

# Load config từ .env
load_dotenv()

# Nhập các tool từ chatbot_tools.py
from chatbot_tools import (
    get_stock_price_and_indicators,
    get_ai_score,
    get_top_k_stocks,
    get_wyckoff_status,
    get_risk_warnings,
    get_sector_comparison,
    get_full_stock_analysis
)

# 1. Cấu hình LLM kết nối với ai-box.vn (Claude API)
api_key = os.environ.get("AIBOX_API_KEY")
api_base = os.environ.get("AIBOX_API_BASE", "https://api.ai-box.vn/v1")
model_name = os.environ.get("AIBOX_MODEL", "claude-sonnet-4.5")

if not api_key:
    raise ValueError("[ERROR] Khong tim thay AIBOX_API_KEY trong file .env!")

print(f"[SYSTEM] Dang khoi tao LLM {model_name} qua base url {api_base}...")
llm = ChatOpenAI(
    model=model_name,
    temperature=0.0, # Độ sáng tạo = 0 để tránh bot bịa đặt số liệu tài chính
    openai_api_key=api_key,
    openai_api_base=api_base,
    streaming=True
)

# 2. Đóng gói các hàm xử lý dữ liệu thành các Tool của Agent
tools = [
    Tool(
        name="Get_Stock_Price_And_Indicators",
        func=get_stock_price_and_indicators,
        description=(
            "Hữu ích khi người dùng hỏi về thông tin giao dịch của một mã cổ phiếu cụ thể. "
            "Trả về giá đóng cửa gần nhất, tỷ lệ tăng giảm, khối lượng giao dịch so với trung bình 20 phiên, "
            "chỉ báo SMA(20) giá và chỉ báo RSI(14)."
        )
    ),
    Tool(
        name="Get_AI_Prediction_Score",
        func=get_ai_score,
        description=(
            "Hữu ích khi muốn biết điểm số AI (AI Score), khuyến nghị tín hiệu (BUY/SELL/HOLD) "
            "và xu hướng dự báo của mô hình Machine Learning (LightGBM/XGBoost Ensemble) cho một mã."
        )
    ),
    Tool(
        name="Get_Top_K_Stocks",
        func=lambda k_str: get_top_k_stocks(int(k_str) if k_str.isdigit() else 10),
        description=(
            "Hữu ích khi người dùng yêu cầu lọc ra danh sách các mã mạnh nhất thị trường hiện tại "
            "(Ví dụ: 'Top 10 cổ phiếu mạnh nhất', 'Mã nào tốt nhất hôm nay'). Nhận đầu vào là một số nguyên k."
        )
    ),
    Tool(
        name="Get_Wyckoff_And_VSA_Status",
        func=get_wyckoff_status,
        description=(
            "Hữu ích khi cần phân tích kỹ thuật sâu về hành vi giá theo trường phái Wyckoff và VSA. "
            "Trả về Pha hiện tại (Phase A/B/C/D/E), biên độ Trading Range, vùng giá mua đề xuất, "
            "các sự kiện Wyckoff, tín hiệu VSA và ĐIỀU KIỆN CHUYỂN VỊ THẾ TỪ HOLD SANG BUY chi tiết."
        )
    ),
    Tool(
        name="Get_Market_Risk_Warnings",
        func=lambda x: get_risk_warnings(),
        description=(
            "Hữu ích khi người dùng hỏi về cảnh báo rủi ro, các mã bị bán tháo, gãy nền "
            "hoặc danh sách các mã yếu nhất thị trường hôm nay."
        )
    ),
    Tool(
        name="Get_Sector_Relative_Strength_Comparison",
        func=get_sector_comparison,
        description=(
            "Hữu ích khi cần so sánh sức mạnh tương đối (Relative Strength) và hiệu suất "
            "của cổ phiếu với các mã khác trong cùng ngành (industry) trong 5 phiên và 20 phiên gần nhất."
        )
    ),
    Tool(
        name="Get_Full_Stock_Analysis_Data",
        func=get_full_stock_analysis,
        description=(
            "TỐI ƯU NHẤT khi người dùng hỏi về phân tích, nhận định hoặc đánh giá một mã cụ thể (Ví dụ: 'Đánh giá HPG', 'HPG thế nào?'). "
            "Trả về toàn bộ dữ liệu giá, RSI/SMA20, điểm AI score, trạng thái Wyckoff/VSA (bao gồm điều kiện giao dịch) và so sánh ngành cùng lúc để giảm thời gian phản hồi xuống tối thiểu."
        )
    )
]

# 3. Định nghĩa Prompt Hệ thống (System Prompt) cho Agent
system_prompt = """Bạn là một Chatbot tư vấn tài chính và phân tích chứng khoán chuyên nghiệp tại Việt Nam.
Nhiệm vụ của bạn là hỗ trợ nhà đầu tư ra quyết định giao dịch dựa trên số liệu thực tế được cung cấp bởi các Công cụ (Tools).

HƯỚNG DẪN TƯ DUY VÀ 5 CHẾ ĐỘ HOẠT ĐỘNG:
1. Phân tích mã cụ thể (Mode 1):
   Khi nhận câu hỏi về một mã (ví dụ: "VNM thế nào?"), hãy gọi DUY NHẤT công cụ `Get_Full_Stock_Analysis_Data` để lấy toàn bộ dữ liệu tổng hợp (giá, AI score, Wyckoff/VSA, và so sánh ngành) cùng lúc nhằm giảm số lượng bước gọi API xuống tối thiểu và tăng tốc thời gian phản hồi. Tránh gọi các tool đơn lẻ khác trừ khi tool gộp này lỗi.
   Sau đó tổng hợp cả 4 nguồn dữ liệu này để đưa ra nhận định đa chiều. KHÔNG được thiếu phân tích relative strength so với ngành và hướng dẫn chi tiết điều kiện chuyển đổi vị thế HOLD sang BUY (cụ thể volume thế nào, Spring hay SOS bứt phá ra sao dựa trên dữ liệu VSA/Wyckoff).

2. Lọc Top-k hàng ngày (Mode 2):
   Khi nhận câu hỏi lọc mã (ví dụ: "Mã nào mạnh nhất sáng nay?"), hãy gọi tool `Get_Top_K_Stocks`. Trình bày danh sách rõ ràng kèm giải thích ngắn gọn dựa trên số liệu trả về.

3. So sánh nhiều mã (Mode 3):
   Khi được yêu cầu so sánh (ví dụ: "SSI và VND con nào ngon hơn?"), hãy gọi công cụ `Get_Full_Stock_Analysis_Data` cho TỪNG mã (Ví dụ: một lần cho SSI, một lần cho VND) để lấy dữ liệu đồng thời, sau đó đặt lên bàn cân so sánh điểm số AI, dòng tiền, sức mạnh so với ngành và chỉ báo kỹ thuật để đưa ra lựa chọn khách quan.

4. Cảnh báo rủi ro (Mode 4):
   Khi hỏi về rủi ro hoặc mã nguy hiểm, hãy gọi tool `Get_Market_Risk_Warnings`.

5. Giải đáp chung về mô hình & độ tin cậy (Mode 5):
   Khi người dùng hỏi về hiệu suất chung của hệ thống, độ tin cậy, độ chính xác (Confidence Score / Accuracy / Rank IC / RMSE) hoặc các câu hỏi tổng quan về mô hình:
   - Đừng chỉ yêu cầu họ nhập mã cụ thể. Hãy giải thích trực tiếp cho họ 2 hướng:
     a) Điểm tin cậy của từng cổ phiếu (AI Score): Là điểm số dự báo percentile thứ hạng mức vượt trội alpha 5 phiên tới của mã đó so với VNIndex, nằm trong khoảng 0.0 - 1.0 (thực tế từ 0.45 - 0.55). Điểm càng cao (> 0.52) thì độ tự tin xếp hạng mua càng cao (TOP_STRONG).
     b) Độ tin cậy của toàn bộ mô hình (Model Performance): Hệ thống sử dụng mô hình LightGBM Regressor học trên dữ liệu Alpha158. Độ chính xác của mô hình xếp hạng chứng khoán được đánh giá qua chỉ số Rank IC (Information Coefficient - tương quan Spearman) trung bình đạt 0.02 - 0.06 (mức tối ưu cho mô hình định lượng chứng khoán thực chiến) và có kèm theo bộ lọc Market Regime Filter để tự động phòng thủ khi VNIndex xấu.

NGUYÊN TẮC CỐT LÕI:
- KHÔNG tự bịa ra số liệu giá, chỉ báo hoặc điểm số nếu tool không trả về. Nếu không có dữ liệu, hãy báo thật thà.
- Trả lời bằng Tiếng Việt, trình bày chuyên nghiệp, sử dụng markdown để in đậm, gạch đầu dòng các thông tin quan trọng.
- Cuối câu trả lời luôn đưa ra khuyến cáo: "Phân tích trên chỉ mang tính tham khảo dựa trên mô hình AI và chỉ báo kỹ thuật, không phải lời khuyên đầu tư tài chính trực tiếp."
"""

# 4. Cấu hình bộ nhớ đệm (Conversation Memory) để chatbot nhớ ngữ cảnh chat trước đó
memory = ConversationBufferMemory(
    memory_key="chat_history",
    return_messages=True
)

# 5. Khởi tạo Agent
# Sử dụng agent type: chat-conversational-react-description phù hợp cho hội thoại tự nhiên và gọi tools
agent_executor = initialize_agent(
    tools=tools,
    llm=llm,
    agent="chat-conversational-react-description",
    verbose=True, # Hiển thị quá trình LLM suy nghĩ và gọi tool trên console
    memory=memory,
    agent_kwargs={
        "system_message": system_prompt
    },
    handle_parsing_errors=True # Tự động xử lý nếu LLM sinh ra định dạng JSON lỗi khi gọi tool
)

def chat_with_agent(message: str) -> str:
    """Hàm wrapper để gọi agent từ Backend API hoặc Test script."""
    try:
        response = agent_executor.run(input=message)
        return response
    except Exception as e:
        return f"[ERROR] Gặp lỗi khi xử lý câu hỏi: {str(e)}"

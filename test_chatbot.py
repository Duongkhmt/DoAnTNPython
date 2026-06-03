import sys
import logging

# Tắt log debug của langchain để console trông sạch sẽ hơn
logging.getLogger("langchain").setLevel(logging.WARNING)

from chatbot_agent import chat_with_agent

def main():
    print("=============================================================")
    print("🤖 CHATBOT PHÂN TÍCH CỔ PHIẾU THÔNG MINH (CLAUDE API + DB) 🤖")
    print("=============================================================")
    print("Bạn có thể đặt câu hỏi về các cổ phiếu (ví dụ: 'Đánh giá HPG',")
    print("'Top 5 cổ phiếu hôm nay', 'So sánh SSI và VND', 'Cảnh báo rủi ro hôm nay').")
    print("Gõ 'exit' hoặc 'quit' để thoát.\n")

    while True:
        try:
            user_input = input("You: ")
            if user_input.strip().lower() in ["exit", "quit"]:
                print("Tạm biệt!")
                break
            
            if not user_input.strip():
                continue
                
            print("\n🤖 Bot đang phân tích dữ liệu và suy nghĩ...")
            response = chat_with_agent(user_input)
            print(f"\nBot:\n{response}")
            print("=" * 60 + "\n")
        except KeyboardInterrupt:
            print("\nThoát chương trình.")
            break
        except Exception as e:
            print(f"\n❌ Lỗi hệ thống: {e}\n")

if __name__ == "__main__":
    main()

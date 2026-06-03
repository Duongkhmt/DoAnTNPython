import pandas as pd
from sqlalchemy import text
from timescale_utils import DatabaseManager

db_manager = DatabaseManager()

def get_company_info(symbol: str) -> dict:
    """Lấy thông tin cơ bản của công ty từ bảng listing."""
    if db_manager.engine is None:
        return {}
    query = """
        SELECT organ_name, exchange, industry, sector 
        FROM listing 
        WHERE symbol = :symbol
    """
    try:
        with db_manager.engine.connect() as conn:
            result = conn.execute(text(query), {"symbol": symbol.upper()}).first()
            if result:
                return {
                    "organ_name": result[0],
                    "exchange": result[1],
                    "industry": result[2],
                    "sector": result[3]
                }
    except Exception as e:
        print(f"Error fetching company info: {e}")
    return {}

def get_stock_price_and_indicators(symbol: str) -> str:
    """
    Tool 1: Query TimescaleDB -> OHLCV, indicators (RSI, SMA20) theo mã gần đây.
    """
    symbol = symbol.upper()
    if db_manager.engine is None:
        return "Lỗi kết nối cơ sở dữ liệu."
    
    # Lấy thông tin cơ bản
    comp_info = get_company_info(symbol)
    company_name = comp_info.get("organ_name", "Không rõ tên")
    industry = comp_info.get("industry", "Không rõ ngành")
    exchange = comp_info.get("exchange", "HOSE")
    
    # Query 60 phiên gần nhất
    query = """
        SELECT trading_date, open, high, low, close, volume
        FROM quote_history
        WHERE symbol = :symbol
        ORDER BY trading_date DESC
        LIMIT 60
    """
    try:
        with db_manager.engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params={"symbol": symbol})
            
        if df.empty:
            return f"Không tìm thấy dữ liệu giá của mã {symbol} trong database."
        
        # Sắp xếp lại tăng dần theo ngày để tính Indicator
        df = df.iloc[::-1].reset_index(drop=True)
        
        # Tính SMA20 Volume
        df["vol_sma20"] = df["volume"].rolling(20).mean()
        # Tính SMA20 Price
        df["price_sma20"] = df["close"].rolling(20).mean()
        
        # Tính RSI(14)
        delta = df["close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        df["rsi"] = 100 - (100 / (1 + rs))
        
        # Lấy phiên gần nhất và phiên liền trước
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        
        pct_change = ((latest["close"] - prev["close"]) / prev["close"]) * 100
        vol_change_pct = ((latest["volume"] - latest["vol_sma20"]) / latest["vol_sma20"]) * 100 if latest["vol_sma20"] > 0 else 0
        
        result_str = (
            f"=== THÔNG TIN GIAO DỊCH {symbol} ({company_name} - Sàn {exchange}) ===\n"
            f"Phân khúc/Ngành: {industry}\n"
            f"Ngày giao dịch gần nhất: {latest['trading_date']}\n"
            f"Giá đóng cửa gần nhất: {latest['close']:,.0f} VND ({pct_change:+.2f}% so với phiên trước)\n"
            f"Giá mở cửa: {latest['open']:,.0f} | Cao nhất: {latest['high']:,.0f} | Thấp nhất: {latest['low']:,.0f}\n"
            f"Khối lượng phiên: {latest['volume']:,} cổ phiếu\n"
            f"Khối lượng TB 20 phiên: {latest['vol_sma20']:,.0f} cổ phiếu ({vol_change_pct:+.1f}% so với TB)\n"
            f"Chỉ báo SMA(20) Giá: {latest['price_sma20']:,.0f} VND (Giá đang {'TRÊN' if latest['close'] > latest['price_sma20'] else 'DƯỚI'} đường SMA20)\n"
            f"Chỉ báo RSI(14): {latest['rsi']:.2f}\n"
        )
        return result_str
        
    except Exception as e:
        return f"Lỗi khi xử lý dữ liệu giá cho {symbol}: {str(e)}"

def get_ai_score(symbol: str) -> str:
    """
    Tool 2: Lấy điểm dự đoán từ mô hình AI (LightGBM/XGBoost) từ bảng ml_predictions.
    """
    symbol = symbol.upper()
    if db_manager.engine is None:
        return "Lỗi kết nối cơ sở dữ liệu."
        
    query = """
        SELECT predict_date, target_date, ai_score, ai_signal, trend, model_used
        FROM ml_predictions
        WHERE symbol = :symbol
        ORDER BY predict_date DESC
        LIMIT 1
    """
    try:
        with db_manager.engine.connect() as conn:
            result = conn.execute(text(query), {"symbol": symbol}).first()
            
        if not result:
            return f"Hiện chưa có điểm dự báo AI cho mã {symbol} trong bảng `ml_predictions`."
            
        predict_date, target_date, ai_score, ai_signal, trend, model_used = result
        
        # Định vị thứ hạng của mã này trong ngày dự đoán đó
        rank_query = """
            SELECT rank_pct FROM (
                SELECT symbol,
                       PERCENT_RANK() OVER (ORDER BY ai_score DESC) as rank_pct
                FROM ml_predictions
                WHERE predict_date = :predict_date
            ) t WHERE symbol = :symbol
        """
        rank_pct_str = "Không xác định"
        with db_manager.engine.connect() as conn:
            result_rank = conn.execute(text(rank_query), {"predict_date": predict_date, "symbol": symbol}).first()
            if result_rank:
                rank_val = result_rank[0]
                rank_pct_str = f"Top {(rank_val * 100):.1f}% thị trường"
                
        signal_desc = {
            "BUY": "MUA (Tín hiệu tích cực)",
            "STRONG_BUY": "MUA MẠNH (Tín hiệu rất mạnh)",
            "SELL": "BÁN (Tín hiệu tiêu cực)",
            "STRONG_SELL": "BÁN MẠNH (Rủi ro cao)",
            "HOLD": "NẮM GIỮ / THEO DÕI"
        }.get(ai_signal.upper() if ai_signal else "", ai_signal or "NẰM GIỮ")
        
        result_str = (
            f"=== DỰ BÁO AI CHO MÃ {symbol} ===\n"
            f"Ngày chạy dự báo: {predict_date}\n"
            f"Điểm số AI (AI Score): {ai_score:.4f} (Thứ hạng: {rank_pct_str})\n"
            f"Khuyến nghị hệ thống: {signal_desc}\n"
            f"Xu hướng dự báo: {trend or 'Không rõ'}\n"
            f"Mô hình sử dụng: {model_used or 'Ensemble Regressor'}\n"
        )
        return result_str
        
    except Exception as e:
        return f"Lỗi khi truy vấn dự báo AI cho {symbol}: {str(e)}"

def get_top_k_stocks(k: int = 10) -> str:
    """
    Tool 3: Top-k hàng ngày. Lấy k cổ phiếu có điểm dự báo AI Score cao nhất của ngày gần nhất.
    """
    if db_manager.engine is None:
        return "Lỗi kết nối cơ sở dữ liệu."
        
    # Lấy ngày dự báo gần nhất trong bảng ml_predictions
    date_query = "SELECT MAX(predict_date) FROM ml_predictions"
    try:
        with db_manager.engine.connect() as conn:
            latest_date = conn.execute(text(date_query)).scalar()
            
        if not latest_date:
            return "Chưa có dữ liệu dự báo nào trong bảng `ml_predictions`."
            
        query = """
            SELECT p.symbol, l.organ_name, p.ai_score, p.ai_signal, p.trend, l.industry
            FROM ml_predictions p
            LEFT JOIN listing l ON p.symbol = l.symbol
            WHERE p.predict_date = :latest_date
            ORDER BY p.ai_score DESC
            LIMIT :k
        """
        with db_manager.engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params={"latest_date": latest_date, "k": k})
            
        if df.empty:
            return f"Không có dữ liệu dự báo cho ngày {latest_date}."
            
        res_str = f"=== TOP {k} CỔ PHIẾU MẠNH NHẤT HỆ THỐNG AI (Ngày dự báo: {latest_date}) ===\n"
        for idx, row in df.iterrows():
            res_str += (
                f"{idx+1}. {row['symbol']} - {row['organ_name'] or 'Tên công ty'} (Ngành: {row['industry'] or 'N/A'})\n"
                f"   • AI Score: {row['ai_score']:.4f} | Khuyến nghị: {row['ai_signal']} | Xu hướng: {row['trend']}\n"
            )
        return res_str
        
    except Exception as e:
        return f"Lỗi khi lấy danh sách Top {k} cổ phiếu: {str(e)}"

def get_wyckoff_status(symbol: str) -> str:
    """
    Tool 4: Đọc trạng thái Wyckoff và tín hiệu VSA (Volume Spread Analysis) từ bảng wyckoff_analysis.
    """
    symbol = symbol.upper()
    if db_manager.engine is None:
        return "Lỗi kết nối cơ sở dữ liệu."
        
    query = """
        SELECT phase, schematic, tr_low, tr_high, last_close, last_date, risk_reward, data_json
        FROM wyckoff_analysis
        WHERE symbol = :symbol
    """
    try:
        with db_manager.engine.connect() as conn:
            result = conn.execute(text(query), {"symbol": symbol}).first()
            
        if not result:
            return f"Mã {symbol} hiện chưa được chạy phân tích cấu trúc Wyckoff."
            
        phase, schematic, tr_low, tr_high, last_close, last_date, risk_reward, data_json = result
        
        # Giải mã data_json chứa events và vsa_signals
        import json
        details = {}
        if data_json:
            if isinstance(data_json, str):
                details = json.loads(data_json)
            else:
                details = data_json
                
        events = details.get("events", [])
        vsa_signals = details.get("vsa_signals", [])
        entry_zone = details.get("entry_zone")
        
        # Lấy 3 VSA signals gần nhất
        recent_vsa = vsa_signals[-3:] if vsa_signals else []
        vsa_str = ""
        if recent_vsa:
            vsa_str = "\nCác tín hiệu VSA gần đây:\n"
            for sig in recent_vsa:
                vsa_str += f"  • {sig.get('date')}: {sig.get('desc_vi')} (tại giá {sig.get('price'):,.0f} VND)\n"
        else:
            vsa_str = "\nKhông phát hiện tín hiệu VSA bất thường nào gần đây.\n"
            
        event_str = ""
        if events:
            event_str = "Sự kiện Wyckoff đã phát hiện: " + ", ".join([f"{e.get('kind')} ({e.get('date')})" for e in events])
        else:
            event_str = "Chưa phát hiện sự kiện Wyckoff đặc biệt."
            
        entry_str = "N/A"
        if entry_zone:
            entry_str = f"Vùng mua hợp lý: {entry_zone.get('low'):,.0f} - {entry_zone.get('high'):,.0f} VND ({entry_zone.get('reason')})"
            
        # Thêm hướng dẫn/điều kiện giao dịch Wyckoff/VSA tương ứng với từng pha
        phase_upper = phase.upper() if phase else ""
        guidelines = ""
        if "PHASE A" in phase_upper:
            guidelines = (
                "\nĐIỀU KIỆN CHUYỂN VỊ THẾ TỪ HOLD SANG BUY (WYCKOFF/VSA):\n"
                "  • Do đang ở tích lũy sớm (Phase A), áp lực bán mới bắt đầu chững lại. KHÔNG MUA ĐUỔI.\n"
                "  • Điều kiện chuyển sang BUY: Cần xuất hiện một cú rũ bỏ mạnh mẽ ở Phase C (Spring) với khối lượng cạn kiệt (cho thấy cạn cung hoàn toàn) "
                "hoặc giá bứt phá vượt biên trên TR (Sign of Strength - SOS) với khối lượng lớn tối thiểu gấp 1.5 đến 2 lần trung bình 20 phiên để xác nhận dòng tiền lớn đẩy giá."
            )
        elif "PHASE B" in phase_upper:
            guidelines = (
                "\nĐIỀU KIỆN CHUYỂN VỊ THẾ TỪ HOLD SANG BUY (WYCKOFF/VSA):\n"
                "  • Pha tích lũy trung gian (Phase B) đang xây dựng nguyên nhân tăng giá. Thích hợp nắm giữ (HOLD) hoặc gom hàng từng phần ở vùng biên dưới.\n"
                "  • Điều kiện chuyển sang BUY mạnh: Chờ đợi các nhịp No Supply Test (test cạn cung) hoặc Test of Spring ở Phase C với volume rất thấp, "
                "hoặc điểm mua gia tăng khi giá vượt các kháng cự trung hạn với volume bùng nổ."
            )
        elif "PHASE C" in phase_upper:
            guidelines = (
                "\nĐIỀU KIỆN CHUYỂN VỊ THẾ TỪ HOLD SANG BUY (WYCKOFF/VSA):\n"
                "  • Giai đoạn rũ bỏ / Spring (Phase C) là thời điểm tốt nhất để chuyển sang BUY thăm dò.\n"
                "  • Điều kiện mở BUY: Giá thực hiện Spring (quét thủng hỗ trợ dưới TR rồi nhanh chóng rút chân quay lại TR) với khối lượng thấp (cạn cung) hoặc khối lượng cao rồi đảo chiều nhanh chóng (hấp thụ cung). "
                "Mua gia tăng khi có phiên Test thành công (Test of Spring) với khối lượng cực thấp."
            )
        elif "PHASE D" in phase_upper:
            guidelines = (
                "\nĐIỀU KIỆN CHUYỂN VỊ THẾ TỪ HOLD SANG BUY (WYCKOFF/VSA):\n"
                "  • Cổ phiếu đang bứt phá đi lên trong Trading Range (Phase D). Chuyển sang vị thế BUY.\n"
                "  • Điều kiện mua: Mua tại các phiên kiểm thử lại thành công (Last Point of Support - LPS) với khối lượng thấp, "
                "hoặc mua đuổi khi có nến Breakout SOS vượt hẳn kháng cự biên trên của TR với khối lượng cực lớn (tối thiểu gấp 1.8 lần volume trung bình 20 phiên)."
            )
        elif "PHASE E" in phase_upper:
            guidelines = (
                "\nĐIỀU KIỆN CHUYỂN VỊ THẾ TỪ HOLD SANG BUY (WYCKOFF/VSA):\n"
                "  • Giá đã bứt phá hoàn toàn khỏi TR và bước vào xu hướng tăng (Phase E). Vị thế chủ đạo là BUY/HOLD.\n"
                "  • Điều kiện mua gia tăng: Chờ đợi các nhịp điều chỉnh ngắn hạn (Pullback/Back Up) về các đường hỗ trợ mạnh (SMA20, SMA50) với khối lượng thấp dần, "
                "tránh mua đuổi khi giá đang cách quá xa nền tích lũy."
            )
        elif "DISTRIBUTION" in phase_upper or "PHÂN PHỐI" in phase_upper:
            guidelines = (
                "\nĐIỀU KIỆN GIAO DỊCH (MÔ HÌNH PHÂN PHỐI - DISTRIBUTION):\n"
                "  • Tuyệt đối KHÔNG BUY. Ưu tiên vị thế bán hạ tỷ trọng (SELL) hoặc Short.\n"
                "  • Điều kiện bán quyết liệt: Xuất hiện các nhịp kéo ảo vượt biên trên (Upthrust/UTAD) rồi rút đầu nhanh chóng, "
                "hoặc khi giá gãy hẳn hỗ trợ biên dưới của TR (SOW - Sign of Weakness) với khối lượng lớn đột biến."
            )
        else:
            guidelines = (
                "\nHƯỚNG DẪN GIAO DỊCH CHUYỂN VỊ THẾ (WYCKOFF/VSA):\n"
                "  • Nếu tích lũy: Chờ đợi Spring cạn cung (volume < trung bình) hoặc SOS breakout vượt đỉnh (volume > 1.5x trung bình).\n"
                "  • Nếu phân phối: Bán ngay khi có UTAD (Upthrust) hoặc gãy nền SOW với volume lớn."
            )

        res_str = (
            f"=== PHÂN TÍCH WYCKOFF & VSA CHO MÃ {symbol} ===\n"
            f"Ngày cập nhật: {last_date}\n"
            f"Pha hiện tại (Phase): {phase} (Mô hình: {schematic})\n"
            f"Biên độ Trading Range: Thấp {tr_low:,.0f} VND - Cao {tr_high:,.0f} VND\n"
            f"Giá đóng cửa gần nhất: {last_close:,.0f} VND\n"
            f"Tỷ lệ Risk/Reward đề xuất: {risk_reward or 'N/A'}\n"
            f"Vùng khuyến nghị mua: {entry_str}\n"
            f"{event_str}\n"
            f"{vsa_str}"
            f"{guidelines}"
        )
        return res_str
        
    except Exception as e:
        return f"Lỗi khi lấy thông tin Wyckoff cho {symbol}: {str(e)}"

def get_risk_warnings() -> str:
    """
    Tool 5: Cảnh báo rủi ro tự động (Rung lắc mạnh, Quá mua, Điểm AI chạm đáy).
    """
    if db_manager.engine is None:
        return "Lỗi kết nối cơ sở dữ liệu."
        
    try:
        # 1. Quét tìm Top 10 mã có AI Score THẤP nhất (Nguy hiểm)
        latest_date_query = "SELECT MAX(predict_date) FROM ml_predictions"
        with db_manager.engine.connect() as conn:
            latest_date = conn.execute(text(latest_date_query)).scalar()
            
        bottom_str = ""
        if latest_date:
            bottom_query = """
                SELECT p.symbol, p.ai_score, p.ai_signal
                FROM ml_predictions p
                WHERE p.predict_date = :latest_date
                ORDER BY p.ai_score ASC
                LIMIT 5
            """
            with db_manager.engine.connect() as conn:
                bottom_df = pd.read_sql(text(bottom_query), conn, params={"latest_date": latest_date})
            
            if not bottom_df.empty:
                bottom_str = "⚠️ TOP 5 MÃ CÓ AI SCORE THẤP NHẤT HÔM NAY:\n"
                for idx, row in bottom_df.iterrows():
                    bottom_str += f"  {idx+1}. {row['symbol']} (AI Score: {row['ai_score']:.4f} - Khuyến nghị: {row['ai_signal']})\n"
        
        # 2. Quét tìm mã có dấu hiệu "Gãy nền/Volume bán tháo đột biến"
        # Định nghĩa bán tháo: Volume > 2 * Vol TB 20 phiên VÀ Giá giảm > 3% ở phiên gần nhất
        shakeout_query = """
            WITH latest_prices AS (
                SELECT symbol, trading_date, close, open, volume,
                       LAG(close) OVER(PARTITION BY symbol ORDER BY trading_date) as prev_close,
                       AVG(volume) OVER(PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as vol_sma20
                FROM quote_history
                WHERE trading_date >= NOW() - INTERVAL '30 days'
            )
            SELECT symbol, trading_date, close, volume, prev_close, vol_sma20
            FROM latest_prices
            WHERE trading_date = (SELECT MAX(trading_date) FROM quote_history)
              AND close < prev_close * 0.97
              AND volume > 2.0 * vol_sma20
            LIMIT 5
        """
        with db_manager.engine.connect() as conn:
            selloff_df = pd.read_sql(text(shakeout_query), conn)
            
        selloff_str = ""
        if not selloff_df.empty:
            selloff_str = "\n🔥 CẢNH BÁO BÁN THÁO TRONG PHIÊN (Volume đột biến + Giá giảm mạnh):\n"
            for _, row in selloff_df.iterrows():
                chg = ((row['close'] - row['prev_close']) / row['prev_close']) * 100
                vol_ratio = row['volume'] / row['vol_sma20'] if row['vol_sma20'] > 0 else 0
                selloff_str += f"  • {row['symbol']}: Giá đóng cửa {row['close']:,.0f} VND ({chg:+.2f}%), Vol gấp {vol_ratio:.1f} lần trung bình.\n"
        else:
            selloff_str = "\n✅ Chưa phát hiện mã nào có hiện tượng bán tháo khối lượng lớn đột biến.\n"
            
        res_str = (
            f"=== HỆ THỐNG CẢNH BÁO RỦI RO CHỦ ĐỘNG ===\n"
            f"{bottom_str}"
            f"{selloff_str}"
        )
        return res_str
        
    except Exception as e:
        return f"Lỗi khi phân tích cảnh báo rủi ro: {str(e)}"

def get_sector_comparison(symbol: str) -> str:
    """
    So sánh sức mạnh tương đối (Relative Strength) của cổ phiếu so với ngành (industry) trong 5 phiên và 20 phiên gần nhất.
    """
    symbol = symbol.upper()
    if db_manager.engine is None:
        return "Lỗi kết nối cơ sở dữ liệu."
    
    query = """
        WITH latest_date AS (
            SELECT MAX(trading_date) AS max_date 
            FROM technical_indicators 
            WHERE symbol = :symbol
        ),
        stock_perf AS (
            SELECT t.price_momentum_5, t.price_momentum_20, l.industry
            FROM technical_indicators t
            JOIN listing l ON t.symbol = l.symbol
            JOIN latest_date d ON t.trading_date = d.max_date
            WHERE t.symbol = :symbol
        ),
        sector_perf AS (
            SELECT 
                AVG(t.price_momentum_5) AS avg_sector_mom5,
                AVG(t.price_momentum_20) AS avg_sector_mom20,
                COUNT(*) as sector_count
            FROM technical_indicators t
            JOIN listing l ON t.symbol = l.symbol
            JOIN latest_date d ON t.trading_date = d.max_date
            WHERE l.industry = (SELECT industry FROM stock_perf)
              AND t.symbol <> 'VNINDEX'
        )
        SELECT 
            s.price_momentum_5, 
            s.price_momentum_20, 
            s.industry,
            sec.avg_sector_mom5, 
            sec.avg_sector_mom20,
            sec.sector_count
        FROM stock_perf s
        CROSS JOIN sector_perf sec
    """
    try:
        with db_manager.engine.connect() as conn:
            result = conn.execute(text(query), {"symbol": symbol}).first()
            if not result or not result[2]:
                return f"Không tìm thấy thông tin so sánh ngành cho mã {symbol}."
            
            stock_mom5, stock_mom20, industry, sector_mom5, sector_mom20, sector_count = result
            
            # Xử lý None
            stock_mom5 = stock_mom5 or 0.0
            stock_mom20 = stock_mom20 or 0.0
            sector_mom5 = sector_mom5 or 0.0
            sector_mom20 = sector_mom20 or 0.0
            
            diff5 = (stock_mom5 - sector_mom5) * 100
            diff20 = (stock_mom20 - sector_mom20) * 100
            
            status5 = "MẠNH HƠN" if diff5 > 0 else "YẾU HƠN"
            status20 = "MẠNH HƠN" if diff20 > 0 else "YẾU HƠN"
            
            res_str = (
                f"=== SO SÁNH SỨC MẠNH VỚI NGÀNH {industry.upper()} (Gồm {sector_count} mã) ===\n"
                f"• Hiệu suất 5 phiên gần nhất:\n"
                f"  - {symbol}: {stock_mom5*100:+.2f}%\n"
                f"  - Trung bình Ngành: {sector_mom5*100:+.2f}%\n"
                f"  -> Kết quả: {symbol} đang {status5} ngành ({diff5:+.2f}%).\n"
                f"• Hiệu suất 20 phiên gần nhất (Xu xu hướng trung hạn):\n"
                f"  - {symbol}: {stock_mom20*100:+.2f}%\n"
                f"  - Trung bình Ngành: {sector_mom20*100:+.2f}%\n"
                f"  -> Kết quả: {symbol} đang {status20} ngành ({diff20:+.2f}%).\n"
            )
            return res_str
    except Exception as e:
        return f"Lỗi so sánh ngành cho {symbol}: {str(e)}"

def get_full_stock_analysis(symbol: str) -> str:
    """
    Truy vấn toàn bộ dữ liệu phân tích kỹ thuật, điểm AI, trạng thái Wyckoff/VSA, và so sánh sức mạnh ngành của một mã cổ phiếu cụ thể cùng lúc để tối ưu thời gian phản hồi.
    """
    symbol = symbol.upper()
    price_info = get_stock_price_and_indicators(symbol)
    ai_info = get_ai_score(symbol)
    wyckoff_info = get_wyckoff_status(symbol)
    sector_info = get_sector_comparison(symbol)
    
    combined = (
        f"=== DỮ LIỆU TỔNG HỢP CHO MÃ {symbol} ===\n\n"
        f"{price_info}\n"
        f"{ai_info}\n"
        f"{wyckoff_info}\n"
        f"{sector_info}\n"
    )
    return combined



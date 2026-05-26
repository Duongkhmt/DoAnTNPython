# test_evf.py
from vnstock_data import Quote
import pandas as pd
from openpyxl.utils import get_column_letter

symbol = "EVF"

# Dùng VCI thay vì VND — hỗ trợ cả 1D và 1W
q = Quote(symbol=symbol, source="VCI")

# Daily 200 rows
df_daily = q.history(start="2024-01-01", end="2026-05-15", interval="1D")
df_daily = df_daily.tail(200).reset_index(drop=True)
df_daily["time"] = pd.to_datetime(df_daily["time"]).dt.strftime("%Y-%m-%d")
print(f"Daily: {len(df_daily)} rows")

# Weekly 100 rows — VCI hỗ trợ 1W
df_weekly = q.history(start="2022-01-01", end="2026-05-15", interval="1W")
df_weekly = df_weekly.tail(100).reset_index(drop=True)
df_weekly["time"] = pd.to_datetime(df_weekly["time"]).dt.strftime("%Y-%m-%d")
print(f"Weekly: {len(df_weekly)} rows")

# Xuất Excel + auto fit cột
def auto_fit(ws):
    for col_cells in ws.columns:
        max_len = max(len(str(cell.value or "")) for cell in col_cells)
        ws.column_dimensions[get_column_letter(col_cells[0].column)].width = max_len + 4

with pd.ExcelWriter("EVF_history.xlsx", engine="openpyxl") as writer:
    df_daily.to_excel(writer,  sheet_name="Daily_200",  index=False)
    df_weekly.to_excel(writer, sheet_name="Weekly_100", index=False)
    auto_fit(writer.sheets["Daily_200"])
    auto_fit(writer.sheets["Weekly_100"])

print("Done! File: EVF_history.xlsx")
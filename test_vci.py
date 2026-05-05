import sys
docker_venv_path = "e:/DoAnPython/POSTGRESQL_GUIDE/venv/Lib/site-packages"
if docker_venv_path not in sys.path:
    sys.path.append(docker_venv_path)

from vnstock_data import Trading

print("Testing VCI trading for AAV")
tr = Trading(symbol="AAV", source="VCI")
try:
    df_f = tr.foreign_trade(start="2024-01-01", end="2024-01-05")
    print("foreign_trade output:", type(df_f))
except Exception as e:
    print("foreign_trade error:", type(e), e)

try:
    df_p = tr.prop_trade(start="2024-01-01", end="2024-01-05")
    print("prop_trade output:", type(df_p))
except Exception as e:
    print("prop_trade error:", type(e), e)

print("Testing VCI trading for VNG")
tr2 = Trading(symbol="VNG", source="VCI")
try:
    df_f2 = tr2.foreign_trade(start="2024-01-01", end="2024-01-05")
    print("foreign_trade output:", type(df_f2))
except Exception as e:
    print("foreign_trade error:", type(e), e)

try:
    df_p2 = tr2.prop_trade(start="2024-01-01", end="2024-01-05")
    print("prop_trade output:", type(df_p2))
except Exception as e:
    print("prop_trade error:", type(e), e)

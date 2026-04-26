import requests

url = "https://s.cafef.vn/Ajax/PageNew/DataHistory/PutthroughData.ashx"
params = {
    "Symbol": "ACB",
    "StartDate": "01/01/2024",
    "EndDate": "25/04/2026",
    "PageIndex": 1,
    "PageSize": 20,
}
r = requests.get(url, params=params)
print(r.status_code)
print(r.json())
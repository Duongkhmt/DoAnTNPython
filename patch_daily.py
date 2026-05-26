import sys

with open(r'e:\DoAnPython\POSTGRESQL_GUIDE\daily_predict.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('def run_daily_prediction(\n    model_path: str = DEFAULT_MODEL_PATH,\n    features_path: str = DEFAULT_FEATURES_PATH,\n    model_version: str = DEFAULT_MODEL_VERSION,\n) -> dict[str, Any]:', 
                          'def run_daily_prediction(\n    model_path: str = DEFAULT_MODEL_PATH,\n    features_path: str = DEFAULT_FEATURES_PATH,\n    model_version: str = DEFAULT_MODEL_VERSION,\n    target_date: str = None,\n) -> dict[str, Any]:')

content = content.replace('source = fetch_latest_indicator_frame(engine)', 
                          'source = fetch_indicator_frame(engine, target_date)\n    if source.empty:\n        print(f"Khong co du lieu cho ngay {target_date or \'latest\'}")\n        return {}')

content = content.replace('parser.add_argument("--model-version", default=DEFAULT_MODEL_VERSION, help="Model version de luu vao DB")\n    args = parser.parse_args()\n    run_daily_prediction(args.model, args.features, args.model_version)', 
                          'parser.add_argument("--model-version", default=DEFAULT_MODEL_VERSION, help="Model version de luu vao DB")\n    parser.add_argument("--start-date", help="Ngay bat dau backfill (YYYY-MM-DD)")\n    parser.add_argument("--end-date", help="Ngay ket thuc backfill (YYYY-MM-DD)")\n    args = parser.parse_args()\n    \n    if args.start_date and args.end_date:\n        dates = pd.date_range(start=args.start_date, end=args.end_date, freq=\'B\')\n        for d in dates:\n            date_str = d.strftime("%Y-%m-%d")\n            print(f"\\n--- Chay du doan cho ngay {date_str} ---")\n            run_daily_prediction(args.model, args.features, args.model_version, target_date=date_str)\n    else:\n        run_daily_prediction(args.model, args.features, args.model_version)')

with open(r'e:\DoAnPython\POSTGRESQL_GUIDE\daily_predict.py', 'w', encoding='utf-8') as f:
    f.write(content)

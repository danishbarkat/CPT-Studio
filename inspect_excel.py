import pandas as pd

file_path = "/Volumes/Transcend/DEAN-new idea/130% of Mcare24 (3).xlsx"
try:
    df = pd.read_excel(file_path, sheet_name='Fee_Schedule_Extract_Query_Mult')
    print("Columns found:")
    for col in df.columns:
        print(f"'{col}'")
    print("\nFirst few rows:")
    print(df.head())
except Exception as e:
    print(f"Error reading excel: {e}")

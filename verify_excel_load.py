from app import CPTPricingAnalyzer
import pandas as pd
import os

def test_excel_loading():
    analyzer = CPTPricingAnalyzer()
    file_path = "/Volumes/Transcend/DEAN-new idea/130% of Mcare24 (3).xlsx"
    
    print(f"Testing file: {file_path}")
    
    if not os.path.exists(file_path):
        print("Error: File not found!")
        return

    try:
        # We need to open the file as binary for the load_excel_file method
        with open(file_path, 'rb') as f:
            success, message = analyzer.load_excel_file(f, "Test Source")
            
        print(f"Success: {success}")
        print(f"Message: {message}")
        
        if success:
            cpt_data = analyzer.cpt_pricing.get("Test Source", {})
            print(f"Loaded {len(cpt_data)} CPT codes.")
            # Print a sample
            first_code = list(cpt_data.keys())[0]
            print(f"Sample Code: {first_code}")
            print(f"Sample Data: {cpt_data[first_code]}")
            
    except Exception as e:
        print(f"Exception during test: {e}")

if __name__ == "__main__":
    test_excel_loading()

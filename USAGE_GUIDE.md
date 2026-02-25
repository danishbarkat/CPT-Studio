# 🎯 How to Use the CPT Pricing Comparison Tool

## Step-by-Step Guide

### Step 1: Extract a URL from Your JSON File

Your JSON file contains links to actual pricing data. Here's one you can use:

**Sample URL to try:**
```
https://bcbsil.mrf.bcbs.com/2025-10_320_33B0_in-network-rates_12_of_31.json.gz?&Expires=1763906632&Signature=P4QaBrnndgUsOWBzyjAexkIx1BL1dK--JUBi0yRimACgxMks~mCB-lKFK6jEdhD~pAJd-9ZaIYwmrPMdMYfTPvyviXIOxRox2ycYZwejrA9UuEUiW4LI30A936xs84~7OPlktaAABpPQI8M6ilmESHLHbEIAfmxwc9kFXzugSBQivWab9mL6GKUrHpTqGWs6US1ccfeTAlFSs2h31TbUG6VgbDKdOazE7hytwn1zRBCOfDKZpTLshHAlqWzbUKojqtDIIFwN6lGr-HRvhbf2yyTAHS7I9KgQzUkRQ~HyLaVow967ySeCZ-vpfz-JKJ1xXJRyuhM8lRy1pyNYj8GoMA__&Key-Pair-Id=K27TQMT39R1C8A
```

### Step 2: Load Source 1 (JSON from Blue Cross)

1. Open http://localhost:5000
2. In **Source 1**:
   - Name: "Blue Cross IL"
   - Upload your index JSON file OR paste the file path:
     `/Volumes/Transcend/DEAN-new idea/2025-09-22_Blue-Cross-and-Blue-Shield-of-Illinois_index.json`
3. Click "Load Source 1"
4. You'll see a list of in-network files
5. **Click on one of the files** to fetch actual CPT codes

### Step 3: Load Source 2 (Excel Spreadsheet)

1. In **Source 2**:
   - Name: "My Pricing"
   - Upload an Excel file with columns:
     - `CPT Code` or `Code`
     - `Price` or `Rate` or `Amount`
     - `Description` (optional)
2. Click "Load Source 2"

**Sample Excel file created:** `sample_cpt_pricing.xlsx`

### Step 4: Compare!

1. Once both sources are loaded, click the **"🔍 Compare Pricing"** button
2. You'll see:
   - Total codes compared
   - Which source has higher prices
   - Which source has lower prices (better deal!)
   - Percentage differences
   - Detailed tables showing the comparisons

## What the Tool Does:

✅ **Parses Complex JSON** - Handles the nested structure of insurance pricing files
✅ **Reads Excel Files** - Automatically detects CPT code and price columns
✅ **Fetches Gzipped Data** - Downloads and decompresses the actual pricing files
✅ **Smart Comparison** - Matches CPT codes between sources
✅ **Visual Results** - Shows which provider offers better rates
✅ **Percentage Differences** - Calculates savings potential

## Example Workflow:

1. **Upload Blue Cross JSON** → Get list of pricing files
2. **Click a pricing file** → Loads every CPT code from that file (UI preview shows the first 20 rows)
3. **Upload your Excel** → Loads your pricing data
4. **Click Compare** → See side-by-side comparison!

## For Your Client Pitch:

Show them:
1. How easy it is to upload their data
2. The instant comparison results
3. The potential savings they could find
4. The professional, modern interface

## Next Steps:

- Automatically process ALL in-network files (batch mode)
- Add export to Excel functionality
- Add search/filter by CPT code
- Calculate total potential savings
- Add historical price tracking

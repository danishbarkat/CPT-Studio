# CPT Code Pricing Comparison Tool

A Flask-based web application for comparing CPT (Current Procedural Terminology) code pricing across different healthcare insurance providers.

## Features

- 📁 **Upload JSON Files**: Upload transparency in coverage JSON files from insurance providers
- 🔗 **URL Support**: Provide direct URLs to JSON files
- 🔍 **CPT Code Extraction**: Automatically extracts CPT codes and pricing from nested JSON structures
- 📊 **Price Comparison**: Compare pricing between two different sources
- 💰 **Identify Better Rates**: Quickly see which provider offers better rates for specific procedures

## Understanding the JSON Structure

The JSON file you have (`2025-09-22_Blue-Cross-and-Blue-Shield-of-Illinois_index.json`) is an **index file** that contains:

1. **reporting_entity_name**: The insurance company name
2. **reporting_structure**: Array of plan structures
3. **in_network_files**: Array of URLs pointing to actual pricing data files

Each URL in `in_network_files` points to a **gzipped JSON file** containing:
- **billing_code_type**: Type of code (CPT, HCPCS, etc.)
- **billing_code**: The actual CPT code number
- **description**: Description of the procedure
- **negotiated_rates**: Array of pricing information
  - **negotiated_prices**: The actual prices
    - **negotiated_rate**: The dollar amount
    - **billing_class**: Type of billing (professional, institutional)

## Installation

```bash
# Navigate to the project directory
cd /Volumes/Transcend/DEAN-new\ idea/cpt-pricing-tool

# Install dependencies
pip install -r requirements.txt
```

## Usage

1. **Start the Flask server**:
```bash
python app.py
```

2. **Open your browser** and go to:
```
http://localhost:5000
```

3. **Load Data Sources**:
   - **Option 1**: Upload your JSON file directly
   - **Option 2**: Provide a URL to the JSON file

4. **View In-Network Files**:
   - After loading, you'll see a list of available in-network pricing files
   - Click on any file to fetch and display CPT codes and pricing

5. **Compare Pricing**:
   - Load two different sources
   - The tool will help you identify which source has better rates

## Example: Your JSON File

Your file contains links to **hundreds** of gzipped JSON files. For example:
- "ND Preferred Blue PPO in-network file 12 of 31"
- "NY PPO in-network file 3 of 11"
- etc.

Each of these files contains actual CPT code pricing data. The tool will:
1. Parse your index file
2. Show you all available pricing files
3. Let you click to fetch actual CPT codes
4. Display pricing in an easy-to-read table

## API Endpoints

- `GET /`: Main interface
- `POST /upload`: Upload JSON file or URL
- `POST /fetch_pricing`: Fetch pricing from a specific in-network file URL
- `POST /compare`: Compare pricing between two sources
- `GET /sources`: Get list of loaded sources

## Next Steps for Client Pitch

To make this production-ready for your client:

1. **Add Database**: Store CPT codes and pricing for faster comparisons
2. **Bulk Processing**: Process all in-network files automatically
3. **Export Features**: Export comparison results to Excel/CSV
4. **Advanced Filtering**: Filter by CPT code, price range, provider type
5. **Visualization**: Add charts showing price differences
6. **Authentication**: Add user accounts for saving comparisons
7. **API Integration**: Direct integration with insurance provider APIs

## Technical Notes

- The tool handles gzipped JSON files automatically
- Signed URLs (with `Expires` and `Signature` parameters) may expire
- Processing large files may take time - consider adding background jobs
- Loads every CPT code contained in the selected in-network pricing file (UI preview still shows the first 20 rows for readability)
- Large pricing files are cached locally after the first download so repeated fetches of the same URL load almost instantly

## License

MIT

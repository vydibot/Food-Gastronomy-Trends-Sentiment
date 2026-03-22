# NYC Gastronomic Data Collection Pipeline

This project collects raw recipe data and web scraping data for sentiment analysis of NYC food trends.

## Environment Setup

### API Key Configuration
1. Get your Spoonacular API key from [spoonacular.com](https://spoonacular.com/food-api)
2. Set the environment variable:
   ```bash
   export SPOONACULAR_API_KEY="your_api_key_here"
   ```
3. Or create a `.env` file in the `API use/` directory:
   ```
   SPOONACULAR_API_KEY=your_api_key_here
   ```

## Data Collection Structure

### API Data (Spoonacular Recipes)
- **Location**: `datalake_bronze/API use/responsesapi/`
- **Format**: `{recipe_name}_{timestamp}.json`
- **Example**: `truffle_diet_vegan_20260322_120000.json`

### Web Scraping Data (NYC Food Articles)
- **Location**: `datalake_bronze/web_scraping/responsescraping/`
- **Format**: `foodblog_{topic}_{timestamp}.json`
- **Example**: `foodblog_nycfoodtrends_20260322_120000.json`

## Usage

### Test API Connection
Before collecting data, test your API key:
```bash
cd datalake_bronze/API use/
python test_api.py
```

### Collect Recipe Data
```bash
cd datalake_bronze/API use/
python main.py
```

### Collect Web Scraping Data
```bash
cd datalake_bronze/web_scraping/
python WebScrapping_NY.py
```

## Data Pipeline Flow
1. **Raw Data Collection** (Current Phase)
   - API recipes with dietary restrictions
   - NYC food trend articles

2. **Data Processing** (Next Phase)
   - Clean and normalize data
   - Extract features for sentiment analysis

3. **Sentiment Analysis** (Future Phase)
   - NLP analysis of recipes and articles
   - Correlation with NYC gastronomic trends
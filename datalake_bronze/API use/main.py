from spoonacular_client import SpoonacularAPI
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load API key from environment variable
API_KEY = os.getenv('SPOONACULAR_API_KEY')
if not API_KEY:
    print("Warning: SPOONACULAR_API_KEY environment variable not found.")


api_client = SpoonacularAPI(API_KEY)

def cargar_local(filename: str):
    """Read if a JSON already exists."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return None

def generar_nombre_archivo(recipe_name: str) -> str:
    """Generate the filename for the JSON file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{recipe_name}_{timestamp}.json"

from pathlib import Path


def asegurar_directorio_response():
    """Secure that the va directory be the correct datalake_bronze/API use/."""

    # Ruta absoluta basada en la ubicación del archivo main.py
    project_dir = Path(__file__).resolve().parent
    response_dir = project_dir / "responsesapi"
    response_dir.mkdir(parents=True, exist_ok=True)
    return str(response_dir)

def collect_recipes_by_ingredient(ingredient: str, response_base_dir: str, number: int = 10, intolerances: str = "", diet: str = ""):
    """
    Collect raw recipe data for recipes containing a specific ingredient with dietary restrictions.
    This is for sentiment analysis of NYC gastronomic trends and accommodating all guests.

    :param ingredient: The ingredient to search for in recipes
    :param number: Number of recipes to collect
    :param intolerances: Comma-separated list of intolerances (e.g., "gluten,dairy")
    :param diet: Diet requirements (e.g., "vegetarian" or "vegan|vegetarian" for OR, "gluten free,vegetarian" for AND)
    """
    # Create filename with ingredient and dietary info
    recipe_name = ingredient.replace(' ', '_')
    if diet or intolerances:
        dietary_parts = []
        if diet:
            dietary_parts.append(f"diet_{diet.replace('|', 'OR').replace(',', 'AND')}")
        if intolerances:
            dietary_parts.append(f"intol_{intolerances.replace(',', 'AND')}")
        recipe_name += "_" + "_".join(dietary_parts)

    filename = os.path.join(response_base_dir, generar_nombre_archivo(recipe_name))

    # Check if we already have data for this ingredient with these dietary restrictions
    existing_data = cargar_local(filename)
    if existing_data:
        print(f"Data already exists for {ingredient} with dietary restrictions, skipping API call...")
        return existing_data

    dietary_info = ""
    if diet or intolerances:
        dietary_info = f" (diet: {diet}, intolerances: {intolerances})" if diet and intolerances else f" (diet: {diet})" if diet else f" (intolerances: {intolerances})"

    print(f"Collecting recipes containing '{ingredient}'{dietary_info}...")

    # Prepare API parameters
    api_params = {
        "save_to": filename,
        "query": "",  # Empty query to focus on ingredients
        "includeIngredients": ingredient,
        "addRecipeInformation": True,  # Get additional recipe information
        "number": number,
    }

    # Add dietary restrictions if specified
    if intolerances:
        api_params["intolerances"] = intolerances
    if diet:
        api_params["diet"] = diet

    # Use complex search with includeIngredients and dietary restrictions
    data = api_client.search_recipes_complex(**api_params)

    recipes_found = len(data.get('results', []))
    dietary_desc = f" with dietary restrictions" if diet or intolerances else ""
    print(f"Collected {recipes_found} recipes for ingredient: {ingredient}{dietary_desc}")
    return data

def main():
    """
    Main data collection pipeline for NYC gastronomic sentiment analysis.
    Collects raw recipe data by ingredients (simplified version for substantial dataset collection).
    """
    try:
        # Create response directory first
        response_base_dir = asegurar_directorio_response()

        print("=== NYC GASTRONOMIC DATA COLLECTION PIPELINE ===")
        print("Collecting raw recipe data by ingredients...\n")
        print(f"Data will be saved to: {response_base_dir}")

        # Quick API test
        print("Testing API connection...")
        try:
            test_result = api_client.search_recipes_complex(query="test", number=1, addRecipeInformation=False)
            if not test_result or 'results' not in test_result:
                print("API test failed - please check your API key")
                return
            print("API connection successful!\n")
        except Exception as e:
            print(f"API test failed: {e}")
            print("Please check your API key from: https://spoonacular.com/food-api")
            return

        # Define ingredients of interest for NYC gastronomic trends
        nyc_ingredients = [
            "truffle",
            "lobster",
            "foie gras",
            "caviar",
            "wagyu beef",
            "saffron",
            "vanilla bean",
            "champagne",
            "artisanal cheese",
            "heirloom tomato",
            "microgreens",
            "sea urchin",
            "black truffle",
            "oyster",
            "sake",
            "matcha",
            "espresso",
            "artichoke",
            "fennel"
        ]

        # Define dietary combinations to accommodate all guests
        dietary_combinations = [
            {"diet": "", "intolerances": ""},  # No restrictions - simplified for easier collection
        ]

        collected_data = {}
        total_recipes = 0

        # Collect recipes for each ingredient with different dietary restrictions
        for ingredient in nyc_ingredients:
            ingredient_data = {}

            for dietary_combo in dietary_combinations:
                try:
                    diet_str = dietary_combo["diet"]
                    intol_str = dietary_combo["intolerances"]

                    data = collect_recipes_by_ingredient(
                        ingredient=ingredient,
                        response_base_dir=response_base_dir,
                        number=1,  # Collect 1 recipe per ingredient for substantial dataset
                        intolerances=intol_str,
                        diet=diet_str
                    )

                    # Store data by dietary combination
                    combo_key = f"{diet_str}_{intol_str}" if diet_str or intol_str else "unrestricted"
                    ingredient_data[combo_key] = data
                    total_recipes += len(data.get('results', []))

                except Exception as e:
                    print(f"✗ Error collecting data for {ingredient} with diet '{diet_str}' and intolerances '{intol_str}': {e}")
                    continue

            collected_data[ingredient] = ingredient_data

        print(f"\n=== COLLECTION COMPLETE ===")
        print(f"Collected recipe data for {len(collected_data)} ingredients")
        print(f"Total recipes collected: {total_recipes}")
        print(f"Dietary combinations tested: {len(dietary_combinations)}")
        print("Raw data saved in 'datalake_bronze/API use/responsesapi/' directory")
        print("Format: responsesapi/{recipe_name}_{timestamp}.json")

    except KeyboardInterrupt:
        print("\n\n=== DATA COLLECTION INTERRUPTED ===")
        print("Process stopped by user (Ctrl+C)")
        print("Partial data has been saved to 'datalake_bronze/API use/responsesapi/' directory")
        print("You can resume collection later by running the script again")
        return

if __name__ == "__main__":
    main()
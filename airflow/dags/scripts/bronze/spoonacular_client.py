import requests
import json
import os
from typing import Dict, Any, List, Union

class SpoonacularAPI:
    """
    Client for interacting with the Spoonacular API. Provides methods to search for recipes and get detailed information."""
    BASE_URL = "https://api.spoonacular.com"

    def __init__(self, api_key: str):
        self.api_key = api_key 
        self.headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key
        }

    def _save_to_json(self, data: Union[Dict, List], filename: str) -> None:
        """
        Helper function to save the response to a JSON file.
        """
        # Ensure the file has the .json extension
        if not filename.endswith('.json'):
            filename += '.json'

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            print(f"Results saved to '{filename}'")
        except IOError as e:
            print(f"Error saving file: {e}")

    def search_recipes_complex(self, save_to: str = None, **kwargs) -> Dict[str, Any]:
        """
        Endpoint 1: Complex Search
        Search for recipes by combining filters. You can save the result locally.

        :param save_to: (Optional) Name of the JSON file where to save the data.
        :param kwargs: Optional parameters for the API (e.g., query="pasta", maxFat=25).
        :return: JSON dictionary with the results.
        """
        endpoint = f"{self.BASE_URL}/recipes/complexSearch"

        # Add API key to parameters
        params = {"apiKey": self.api_key}
        params.update(kwargs)

        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            #If a filename was provided, we save the data
            if save_to:
                self._save_to_json(data, save_to)

            return data
        except requests.exceptions.RequestException as e:
            print(f"Error in the complexSearch request: {e}")
            return {}

    def get_recipe_information(self, recipe_id: int, save_to: str = None, **kwargs) -> Dict[str, Any]:
        """
        Get detailed information about a specific recipe including instructions and ingredients.

        :param recipe_id: The ID of the recipe to get information for.
        :param save_to: (Opcional) Nombre del archivo JSON donde guardar los datos.
        :param kwargs: Parámetros opcionales como includeNutrition=False.
        :return: Diccionario JSON con la información detallada de la receta.
        """
        endpoint = f"{self.BASE_URL}/recipes/{recipe_id}/information"

        # Add API key to parameters
        params = {"apiKey": self.api_key, "includeNutrition": False}
        params.update(kwargs)

        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            if save_to:
                self._save_to_json(data, save_to)

            return data
        except requests.exceptions.RequestException as e:
            print(f"Error in the get_recipe_information request for ID {recipe_id}: {e}")
            return {}
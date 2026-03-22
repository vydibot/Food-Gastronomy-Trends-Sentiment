import requests
import json
import os
from typing import Dict, Any, List, Union

class SpoonacularAPI:
    """
    Cliente de Python para interactuar con la API de Spoonacular.
    Incluye guardado local para ahorrar cuota de la API.
    """
    BASE_URL = "https://api.spoonacular.com"

    def __init__(self, api_key: str):
        self.api_key = api_key
        # Spoonacular accepts API key in header or as query parameter
        # Using header method as recommended
        self.headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key
        }

    def _save_to_json(self, data: Union[Dict, List], filename: str) -> None:
        """
        Función auxiliar para guardar la respuesta en un archivo JSON.
        """
        # Aseguramos que el archivo tenga la extensión .json
        if not filename.endswith('.json'):
            filename += '.json'

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            print(f"Resultados guardados localmente en '{filename}'")
        except IOError as e:
            print(f"Error al guardar el archivo: {e}")

    def search_recipes_complex(self, save_to: str = None, **kwargs) -> Dict[str, Any]:
        """
        Endpoint 1: Complex Search
        Busca recetas combinando filtros. Puedes guardar el resultado localmente.

        :param save_to: (Opcional) Nombre del archivo JSON donde guardar los datos.
        :param kwargs: Parámetros opcionales de la API (ej. query="pasta", maxFat=25).
        :return: Diccionario JSON con los resultados.
        """
        endpoint = f"{self.BASE_URL}/recipes/complexSearch"

        # Add API key to parameters
        params = {"apiKey": self.api_key}
        params.update(kwargs)

        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            # Si se proporcionó un nombre de archivo, guardamos los datos
            if save_to:
                self._save_to_json(data, save_to)

            return data
        except requests.exceptions.RequestException as e:
            print(f"[x] Error en la petición complexSearch: {e}")
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
            print(f"Error en la petición get_recipe_information para ID {recipe_id}: {e}")
            return {}
#!/usr/bin/env python3
"""
Test script to verify Spoonacular API key and connection.
Run this before running the main data collection script.
"""

import os
import sys
from dotenv import load_dotenv
from spoonacular_client import SpoonacularAPI

def test_api_connection():
    """Test basic API connectivity with a simple query."""

    # Load environment variables
    load_dotenv()

    # Get API key
    api_key = os.getenv('SPOONACULAR_API_KEY')
    if not api_key:
        print("❌ ERROR: SPOONACULAR_API_KEY not found!")
        print("Please set it with: export SPOONACULAR_API_KEY='your_api_key_here'")
        print("Or create a .env file with: SPOONACULAR_API_KEY=your_api_key_here")
        return False

    print(f"🔑 API Key found: {api_key[:8]}...")
    print("Testing API connection...")

    # Initialize API client
    api_client = SpoonacularAPI(api_key)

    try:
        # Test with a simple query
        result = api_client.search_recipes_complex(
            query="pasta",
            number=1,
            addRecipeInformation=False
        )

        if result and 'results' in result:
            recipes_found = len(result['results'])
            print(f"✅ API connection successful! Found {recipes_found} test recipes.")
            return True
        else:
            print("❌ API returned empty result")
            return False

    except Exception as e:
        print(f"❌ API connection failed: {e}")
        return False

if __name__ == "__main__":
    print("=== Spoonacular API Connection Test ===")
    success = test_api_connection()

    if success:
        print("\n🎉 API is working! You can now run the main data collection script.")
        sys.exit(0)
    else:
        print("\n💥 API test failed. Please check your API key and try again.")
        sys.exit(1)
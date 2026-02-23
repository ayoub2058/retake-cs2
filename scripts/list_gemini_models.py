"""List available Gemini models that support content generation."""

import os
import sys

from dotenv import load_dotenv

load_dotenv(".env.local")

api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    print("GEMINI_API_KEY is not set in .env.local")
    sys.exit(1)

try:
    import google.generativeai as genai
except ImportError:
    print("google-generativeai is not installed. Run: pip install google-generativeai")
    sys.exit(1)

genai.configure(api_key=api_key)

print("Checking available Gemini models...")
try:
    found = 0
    for m in genai.list_models():
        if "generateContent" in (m.supported_generation_methods or []):
            print(f"  OK: {m.name}")
            found += 1
    if found == 0:
        print("No models with generateContent support found.")
    else:
        print(f"\n{found} model(s) available.")
except Exception as e:
    print(f"Error listing models: {e}")
    sys.exit(1)

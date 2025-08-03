import json
from fastapi.openapi.utils import get_openapi

def generate_openapi_schema(app):
    schema = get_openapi(
        title="BookBlend API",
        version="1.0.0",
        description="API Docs for bookblend.io",
        routes=app.routes,
    )
    schema["openapi"] = "3.1.0"  # Optional: force latest spec
    schema["servers"] = [
        {
            "url": "https://book-blend-backend.vercel.app/"
        }
    ]

    with open("docs/openapi.json", "w") as f:
        json.dump(schema, f, indent=2)
    print("✅ Wrote OpenAPI schema to docs/openapi.json")

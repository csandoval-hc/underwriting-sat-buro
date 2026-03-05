from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    syntage_api_key: str
    syntage_base_url: str = "https://api.syntage.com"

def load_settings() -> Settings:
    api_key = os.getenv("SYNTAGE_API_KEY", "").strip()
    base_url = os.getenv("SYNTAGE_BASE_URL", "https://api.syntage.com").strip()

    if not api_key:
        raise ValueError(
            "Falta SYNTAGE_API_KEY. Crea un archivo .env con SYNTAGE_API_KEY=... "
            "o exporta la variable de entorno."
        )

    return Settings(syntage_api_key=api_key, syntage_base_url=base_url)

from __future__ import annotations

import json
from pathlib import Path

from app.main import create_app


def main() -> None:
    output = Path("openapi.json")
    output.write_text(
        json.dumps(create_app().openapi(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(output.resolve())


if __name__ == "__main__":
    main()

from pathlib import Path


SCRIPT_DIR = Path(__file__).parent
ACQUIRE_LUA_SCRIPT = (SCRIPT_DIR / "acquire.lua").read_text()
RELEASE_LUA_SCRIPT = (SCRIPT_DIR / "release.lua").read_text()

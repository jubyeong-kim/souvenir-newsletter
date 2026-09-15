# run.py — 진입점. graph.py 는 불러오기만 해도 안전해야 하므로 실행은 여기서만.
import os, sys
from graph import run

if __name__ == "__main__":
    if "--dry-run" in sys.argv:
        os.environ["DRY_RUN"] = "1"
    out = run()
    for line in out["log"]:
        print(line)                    # 이 출력이 Actions 로그에 그대로 남는다

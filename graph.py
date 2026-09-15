"""뉴스레터 에이전트 — 수집·선별·취재·검수·발행 파이프라인.

step01~step12 에서 만든 것을 한 파일로 합친 것. 같은 이름을 여러 번 정의했던 것은
최종 버전만 남기고, 정의가 먼저 오고 쓰는 곳이 뒤에 오도록 순서를 맞췄다.

이 파일은 **불러오기만 해도 안전해야 한다.** run() 호출을 여기 적으면
`import graph` 하는 순간 브리핑이 발행된다. 실행은 run.py 에서만 한다.
"""
import html, json, operator, os, pathlib, re
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Annotated, TypedDict
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import feedparser, requests, trafilatura, yaml
from openai import OpenAI
from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, START, END
from langgraph.types import Send

try:                                    # 로컬에서는 .env, Actions 에서는 env 로 들어온다
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

client = OpenAI()
MODEL = os.environ.get("MODEL", "gpt-4.1-mini")

# ───────────────────── 설정 (구조와 내용의 경계) ─────────────────────
# 분야에 따라 달라지는 것은 전부 topic.yaml 에 있다. 파이썬에는 구조만 남긴다.
# 파일이 없으면 바로 멈춘다 — 기본값으로 도는 것이야말로 조용한 실패다.
CFG_PATH = pathlib.Path(os.environ.get("TOPIC_CONFIG", "audience.yaml"))
if not CFG_PATH.exists():
    raise SystemExit(f"설정 파일이 없습니다: {CFG_PATH}")
CFG = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))

# ── 오늘의 대륙·국가. 날짜만으로 결정되므로 재현 가능하다.
ROT    = CFG["로테이션"]
_DAYS  = (datetime.now() - datetime(2026, 1, 1)).days
BLOC   = ROT[_DAYS % len(ROT)]                                  # 대륙은 매일 순환
NATION = BLOC["국가"][(_DAYS // len(ROT)) % len(BLOC["국가"])]   # 한 바퀴 돌면 다음 국가


# ───────────────────────── State ─────────────────────────
class Brief(TypedDict):
    hours:     int
    collected: list
    picked:    list
    drafted:   Annotated[list, operator.add]    # 워커들이 나눠 채운다 → 합친다
    verified:  list                             # 검수는 줄이는 일 → 리듀서 없음
    log:       Annotated[list, operator.add]


INIT = {"hours": 24, "collected": [], "picked": [],
        "drafted": [], "verified": [], "log": []}


# ───────────────────────── ① 수집 ─────────────────────────
UA = {"User-Agent": "Mozilla/5.0 (newsletter-agent-course)"}
SOURCES = [(s["이름"], s) for s in CFG["소스"]]
PER_SOURCE_CAP = CFG.get("소스별_상한", 8)   # 한 곳이 후보를 독점하면 상대평가가 거기서만 고른다
TRACKING = ("utm_", "fbclid", "gclid", "ref", "src")
KEYWORDS = re.compile("|".join(re.escape(k) for k in CFG.get("키워드", [])) or r"(?!x)x",
                      re.I) if CFG.get("키워드") else None

# 이미 발행한 것을 기억한다. 뉴스에는 없던 문제다 — 기사는 매일 새로 나지만
# 채용공고는 한 번 올라오면 몇 주씩 열려 있어서, 기억하지 않으면 매일 같은 것을 보낸다.
SEEN_PATH = pathlib.Path("store/seen.json")


def load_seen() -> set:
    try:
        return set(json.loads(SEEN_PATH.read_text(encoding="utf-8")))
    except Exception:
        return set()


def save_seen(seen: set, keep: int = 2000):
    SEEN_PATH.parent.mkdir(exist_ok=True)
    SEEN_PATH.write_text(json.dumps(sorted(seen)[-keep:], ensure_ascii=False), encoding="utf-8")


def strip_tags(s):
    return re.sub(r"<[^>]+>", "", s or "").strip()


def canonical(url: str) -> str:
    """추적용 꼬리표만 떼고 나머지 쿼리는 남긴다.

    ?를 통째로 자르면 기사 번호가 쿼리에 있는 매체(AI타임스의 ?idxno=)가
    전부 한 건으로 뭉쳐서 하루치 50건이 1건이 된다.
    """
    u = urlsplit(url)
    q = [(k, v) for k, v in parse_qsl(u.query) if not k.lower().startswith(TRACKING)]
    return urlunsplit((u.scheme, u.netloc, u.path.rstrip("/"), urlencode(q), ""))


def published_at(entry):
    t = entry.get("published_parsed")
    return datetime(*t[:6], tzinfo=timezone.utc) if t else None


def fetch_rss(name, src):
    """RSS/Atom 피드. 본문은 안 주므로 report 단계에서 따로 추출해야 한다."""
    feed = feedparser.parse(requests.get(src["주소"], headers=UA, timeout=20).content)
    for e in feed.entries:
        yield {"title": e.title, "url": e.link, "source": name, "at": published_at(e),
               "summary": strip_tags(e.get("summary", ""))[:300]}


def fetch_greenhouse(name, src):
    """회사가 스스로 공개하는 공식 채용 보드 API — 사다리 1칸.

    robots.txt 문제가 없고 content=true 로 본문까지 준다. 그래서 이 소스는
    G1(본문)이 수집 시점에 이미 해결된다 — trafilatura 왕복이 사라진다.
    """
    url = f"https://boards-api.greenhouse.io/v1/boards/{src['보드']}/jobs?content=true"
    for j in requests.get(url, headers=UA, timeout=30).json().get("jobs", []):
        t = j.get("first_published") or j.get("updated_at")       # 게시일이 있으면 그쪽
        body = re.sub(r"\s+", " ", html.unescape(strip_tags(j.get("content", "")))).strip()
        yield {"title": j["title"], "url": j["absolute_url"], "source": name,
               "at": datetime.fromisoformat(t).astimezone(timezone.utc) if t else None,
               "summary": f"{j['location']['name']} · "
                          + ", ".join(d.get("name", "") for d in j.get("departments", [])),
               "body": body,                                      # ← 본문을 여기서 이미 얻는다
               "location": j["location"]["name"]}


def _q(src):
    """소스의 쿼리 틀에 오늘의 국가를 끼운다."""
    return src["쿼리"].format(국가=NATION["ko"], country=NATION["en"])


def fetch_tavily(name, src):
    """Tavily 검색 — 국가별 쿼리가 되고 본문(raw_content)까지 한 번에 준다.

    같은 질의에 간헐적으로 0건을 준다(실측: 동일 요청 2회에 0건/14건).
    그래서 3회까지 다시 물어본다. 그래도 0이면 그냥 비우고 나머지 소스로 간다.
    """
    since = (datetime.now(timezone.utc) - timedelta(days=src.get("일수", 30))).strftime("%Y-%m-%d")
    body = {"query": _q(src), "max_results": src.get("건수", 10),
            "include_raw_content": True, "start_date": since,
            # basic 은 스니펫만 준다 — 본문이 600자 문턱을 못 넘어 전량 탈락한다.
            # advanced 로 실측 1/9 → 10/10. 크레딧을 더 쓰지만 이게 없으면 파이프라인이 빈다.
            "search_depth": src.get("깊이", "advanced")}
    if src.get("언어"):
        body["language"] = src["언어"]
    results = []
    for _ in range(3):
        r = requests.post("https://api.tavily.com/search", json=body, timeout=40,
                          headers={"Authorization": f"Bearer {os.environ['TAVILY_API_KEY']}"})
        r.raise_for_status()
        results = r.json().get("results", [])
        if results:
            break
    for x in results:
        text = x.get("raw_content") or x.get("content") or ""
        if len(x.get("title", "")) < 6 or len(text) < 20:
            continue                        # 한두 단어짜리 빈약한 항목을 섞어 보낸다
        at = x.get("published_date")        # 대체로 비어 있다 → collect() 가 수집시각으로 채운다
        yield {"title": x["title"], "url": x["url"], "source": name,
               "at": datetime.fromisoformat(at).astimezone(timezone.utc) if at else None,
               "summary": (x.get("content") or "")[:300],
               "body": text}


def fetch_youtube(name, src):
    """유튜브 — 기념품 하울·브이로그가 실제로 여기 몰려 있다. 날짜가 전부 온다.

    search.list 는 검색 전용 100회/일 버킷을 쓴다. 실행당 1회만 부른다.
    본문은 영상 설명글인데, 하울 영상은 여기에 품목을 나열해 두는 경우가 많다.
    """
    key = os.environ["YOUTUBE_API_KEY"]
    after = (datetime.now(timezone.utc) - timedelta(days=src.get("일수", 90))
             ).strftime("%Y-%m-%dT%H:%M:%SZ")
    s = requests.get("https://www.googleapis.com/youtube/v3/search", timeout=30, params={
        "part": "snippet", "type": "video", "q": _q(src),
        "maxResults": src.get("건수", 10), "order": "relevance", "publishedAfter": after,
        "regionCode": src.get("지역", "KR"), "relevanceLanguage": src.get("언어", "ko"),
        "key": key}).json()
    ids = [i["id"]["videoId"] for i in s.get("items", []) if i.get("id", {}).get("videoId")]
    if not ids:
        return
    v = requests.get("https://www.googleapis.com/youtube/v3/videos", timeout=30, params={
        "part": "snippet", "id": ",".join(ids), "key": key}).json()
    for it in v.get("items", []):
        sn = it["snippet"]
        # 설명글이 곧 원문이다. 짧으면 report 단계에서 어차피 탈락하는데,
        # 그때는 이미 선별 슬롯을 하나 먹은 뒤다(실측: 287자·422자가 매번 자리만 차지).
        # 쓸 수 없는 것은 여기서 내보내지 않아 자리를 본문 있는 소스에 넘긴다.
        if len(sn.get("description") or "") < MIN_BODY:
            continue
        yield {"title": sn["title"], "url": f"https://www.youtube.com/watch?v={it['id']}",
               "source": name,
               "at": datetime.fromisoformat(sn["publishedAt"].replace("Z", "+00:00")),
               "summary": sn.get("description", "")[:300],
               "body": sn.get("description", "")}      # 설명글 전문


FETCHERS = {"rss": fetch_rss, "greenhouse": fetch_greenhouse,
            "tavily": fetch_tavily, "youtube": fetch_youtube}


def collect(s: dict) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=s["hours"])
    already = load_seen()
    items, dead, seen = [], [], set()
    off_topic = stale = repeated = 0
    for name, src in SOURCES:
        try:
            fetched = list(FETCHERS[src["종류"]](name, src))
        except Exception:
            dead.append(name)                   # 한 곳이 죽어도 나머지는 계속
            continue
        for it in fetched:
            at = it.get("at")
            if not at:
                at = it["at"] = datetime.now(timezone.utc)   # 소스가 날짜를 안 주면 수집시각
                it["date_est"] = True                        # footer 에 '날짜 미확인' 표기
            if at < cutoff:
                stale += 1
                continue
            if KEYWORDS and not KEYWORDS.search(it["title"] + " " + it.get("summary", "")):
                off_topic += 1                  # 분야 밖 — 예선 토큰을 여기에 쓰면 안 된다
                continue
            key = canonical(it["url"])
            if key in seen:
                continue
            if key in already:                  # 지난번에 이미 발행한 것
                repeated += 1
                continue
            seen.add(key)
            items.append(it)

    windowed = len(items)
    items.sort(key=lambda x: x["at"], reverse=True)     # 최신 것부터 남긴다
    kept, per = [], {}
    for it in items:
        n = per.get(it["source"], 0)
        if n >= PER_SOURCE_CAP:
            continue
        per[it["source"]] = n + 1
        kept.append(it)

    # 건너뛴 것을 세어 로그에 남긴다. 그냥 continue 하면 그 순간 증거가 사라진다.
    skipped = []
    if off_topic:
        skipped.append(f"분야 밖 {off_topic}")
    if repeated:
        skipped.append(f"기발행 {repeated}")
    if stale:
        skipped.append(f"창 밖 {stale}")
    return {"collected": kept,
            "log": [f"① 수집   {s['hours']}시간 창 · {windowed}건 "
                    f"→ 소스별 상한({PER_SOURCE_CAP}) → {len(kept)}건"
                    + (f" · 건너뜀({', '.join(skipped)})" if skipped else "")
                    + (f" · 응답 없음 {dead}" if dead else "")]}


# ───────────────────────── ② 선별 ─────────────────────────
BATCH  = CFG.get("예선_묶음", 40)          # 예선 묶음 크기
TARGET = CFG["발행"]["건수"]                # 최종 발행 건수
FINAL_CAP = CFG["발행"].get("한곳당_최대", 2)  # 한 곳이 발행 자리를 독차지하지 않게
OVER = CFG["발행"].get("여유분", 3)            # 검수 탈락을 견디는 초과 선별분


class Pick(BaseModel):
    index: int = Field(description="후보 목록에서의 번호")
    reason: str = Field(description="왜 골랐는지 한 문장")
    event: str = Field(description="이 기사가 다루는 사건을 짧은 라벨로. 같은 사건이면 같은 라벨")


class Shortlist(BaseModel):
    picks: list[Pick]


def build_criteria(cfg):                        # 설정 → 프롬프트 문단
    out = [f"독자는 {cfg['독자']['누구']}입니다.",
           f"이미 아는 것: {cfg['독자']['이미_아는_것']}",
           "", "중요도 기준 (위에 있을수록 우선):"]
    out += [f"- {x}" for x in cfg["중요도_기준"]]
    out += ["", "버릴 것:"]
    out += [f"- {x}" for x in cfg["버릴_것"]]
    return "\n".join(out)


CRITERIA = build_criteria(CFG)                  # 코드에 박혀 있던 문자열 자리


def ask_picks(items, n):
    # 제목만 주면 모델이 판단할 재료가 없다. 뉴스는 제목으로 충분했지만
    # 채용은 근무지가 '국내에서 지원 가능한가'를 가르는 결정적 정보다.
    listing = "\n".join(
        f"{i}. [{it['source']}] {it['title']}"
        + (f"  ({it['summary'][:80]})" if it.get("summary") else "")
        for i, it in enumerate(items))
    sys = (f"{CRITERIA}\n\n오늘의 국가는 {NATION['ko']}({NATION['en']})입니다.\n"
           f"아래 목록에서 중요한 순서대로 {n}건을 고르세요.\n"
           f"{NATION['ko']}에서 살 수 있는 기념품을 다룬 기사를 우선 고르세요. "
           "다른 나라 기념품이나 특정 국가와 무관한 기념품 일반론(개론·사전식 글)은 고르지 마세요.\n"
           "같은 사건을 다룬 기사에는 같은 event 라벨을 붙이세요.")
    out = client.chat.completions.parse(
        model=MODEL, temperature=0,
        messages=[{"role": "system", "content": sys},
                  {"role": "user", "content": listing}],
        response_format=Shortlist).choices[0].message.parsed
    return [p for p in out.picks if 0 <= p.index < len(items)]      # 없는 번호는 버린다


def select(s: dict) -> dict:
    items = s["collected"]
    survivors, rounds = [], []
    for i in range(0, len(items), BATCH):       # 예선 — 묶음마다 여덟 건
        chunk = items[i:i + BATCH]
        picks = ask_picks(chunk, 8)
        rounds.append(f"   예선 묶음 {len(chunk)}건 → {len(picks)}건")
        survivors += [chunk[p.index] for p in picks]
    # 본선 — 한 화면에 놓고 고른다. 자리보다 넉넉히 뽑아 두고 아래에서 상한을 건다.
    finals = ask_picks(survivors, TARGET + 3)
    for p in finals:
        survivors[p.index]["event"] = p.event
        survivors[p.index]["why_picked"] = p.reason

    # 한 곳이 발행 자리를 독차지하지 않게 코드로 막는다.
    # 프롬프트로 부탁할 수도 있지만, 세어서 지켜야 하는 규칙은 결국 코드로 세는 수밖에 없다.
    picked, per_src = [], Counter()
    for p in finals:
        it = survivors[p.index]
        if per_src[it["source"]] >= FINAL_CAP:
            continue
        per_src[it["source"]] += 1
        picked.append(it)
        # 취재(본문 부족)와 검수(품목명 불일치)에서 깎인다. 정확히 TARGET 만 뽑으면
        # 한 건만 떨어져도 발행이 미달한다. 여유분을 두고 발행 직전에 자른다.
        if len(picked) == TARGET + OVER:
            break
    return {"picked": picked,
            "log": rounds + [f"② 선별   {len(items)} → 예선 {len(survivors)} → 본선 {len(finals)}"
                             f" → 한곳당 {FINAL_CAP}건 상한 → {len(picked)}건"]}


# ───────────────────────── ③ 취재 ─────────────────────────
class Draft(BaseModel):        # 칸을 나누는 기준은 "무엇과 대조할 수 있는가"
    headline: str = Field(description="20자 내외의 한국어 헤드라인")
    summary:  str = Field(description="세 문장 요약. ~합니다체, 과장 없이 건조하게")
    why:      str = Field(description="여행자 관점의 인사이트 한 문장. "
                          "어디서 사는지·왜 지금인지·가져올 때 주의할 점 중 하나를 원문 근거로 쓸 것")
    item: str = Field(description="기사가 다루는 대표 기념품의 이름. 원문에 나온 표기 그대로")


class ReportIn(TypedDict):                     # 워커가 받는 것은 기사 하나뿐
    item: dict


DESK = "\n".join(f"- {t['이름']}: {t['데스크지침']}" for t in CFG.get("토픽", []))
SYS = (f"당신은 '{CFG['발행']['이름']}' 의 편집자입니다. 독자는 {CFG['독자']['누구']}입니다.\n"
       "아래 원문을 읽고 헤드라인·요약·왜 중요한지를 쓰세요.\n"
       "반드시 한국어로 쓰세요. '주목된다·기대를 모은다' 같은 기자체 표현은 쓰지 마세요.\n"
       "item은 원문에 실제로 나온 표기 그대로 하나만 적으세요. "
       "원문에 없는 이름은 만들지 마세요. 여러 개를 나열하지 마세요.\n"
       + (f"\n데스크 지침:\n{DESK}" if DESK else ""))
MIN_BODY = CFG.get("본문_최소길이", 600)         # 섹션 4에서 정한 G1 기준선


def extract_body(url):
    d = trafilatura.fetch_url(url)
    return trafilatura.extract(d) if d else None


def _call(body, extra=""):
    return client.chat.completions.parse(
        model=MODEL, temperature=0,
        messages=[{"role": "system", "content": SYS + extra},
                  {"role": "user", "content": body[:6000]}],
        response_format=Draft).choices[0].message.parsed


def draft(body):
    """한글이 없으면 한 번 더 요청한다. 프롬프트로는 40% 정도 안 지켜진다."""
    d = _call(body)
    if re.search(r"[가-힣]", d.summary):
        return d, False
    return _call(body, "\n\n중요: 원문이 영어라도 반드시 한국어로 쓰세요."), True


def fan_report(s: dict):                       # 기사 수만큼 워커를 펼친다
    return [Send("report", {"item": it}) for it in s["picked"]]


def report(s: ReportIn) -> dict:
    it = s["item"]
    # 수집 단계가 본문을 이미 준 소스(공식 API·Tavily raw_content·유튜브 설명글)는 왕복을 아낀다.
    # 단, Tavily가 스니펫(150자 안팎)만 준 날이 있다(실측: UAE 질의에서 raw_content 전부 비어 있음).
    # 얇은 본문은 없는 셈 치고 trafilatura에 한 번 더 맡긴다. 그래도 안 되면 제외가 정상이다.
    body = it.get("body")
    if body and len(body) >= MIN_BODY:
        pass
    elif it.get("source") == "유튜브":
        pass                                    # 영상 설명글이 원문 자체라 추출 왕복이 무의미하다
    else:
        body = extract_body(it["url"])
    if not body or len(body) < MIN_BODY:
        return {"drafted": [],
                "log": [f"   취재 제외 {it['source']} · 본문 {len(body or '')}자"]}
    d, retried = draft(body)
    return {"drafted": [{**it, "body": body[:6000], "retried": retried, **d.model_dump()}],
            **({"log": [f"   재요청 {it['source']} · 영어로 나와 한국어로 다시 요청"]}
               if retried else {})}


# ───────────────────────── ④ 검수 ─────────────────────────
class Verdict(BaseModel):
    ok:       bool      = Field(description="요약이 원문에 근거하면 true")
    problems: list[str] = Field(description="근거 없는 부분. 없으면 빈 목록")


SYS_CHECK = ("판정 기준은 두 가지다.\n"
             "1) 요약이 원문에서 뒷받침되는가.\n"
             f"2) 원문이 오늘의 국가({NATION['ko']})에서 파는 기념품 이야기인가.\n"
             "둘 중 하나라도 아니면 ok=false다. "
             "번역이나 단위 환산은 문제가 아니다.")


def check(d):
    user = (f"[원문]\n{d['body'][:5000]}\n\n"
            f"[헤드라인]\n{d['headline']}\n\n[요약]\n{d['summary']}")
    return client.chat.completions.parse(
        model=MODEL, temperature=0,
        messages=[{"role": "system", "content": SYS_CHECK},
                  {"role": "user", "content": user}],
        response_format=Verdict).choices[0].message.parsed


def number_problems(d):
    """숫자 대조. 원문과 요약이 둘 다 한국어일 때만 적용한다.

    영어 원문에 쓰면 three months→3개월을 오탐하지만, 같은 언어끼리는
    오탐 조건이 없다. LLM 검수가 놓치는 표기 오류(2026→20260)를 이쪽이 잡는다.
    """
    if not (re.search(r"[가-힣]", d["body"]) and re.search(r"[가-힣]", d["summary"])):
        return []
    body = d["body"].replace(",", "")
    nums = sorted(set(re.findall(r"\d[\d,\.]*", d["headline"] + " " + d["summary"])))
    return [f"원문에 없는 숫자: {n}" for n in nums if n.replace(",", "") not in body]


def item_problems(d):
    """품목명이 원문에 실제로 있는지. 공백을 지우고 대조한다.

    외부 조회가 아니라 우리가 가져온 원문과의 대조라 오탐이 없다.
    대신 원문 자체가 틀린 경우는 못 잡는다 — 그건 아래 LLM 판정이 맡는다.
    """
    item = d.get("item") or ""
    if not item:
        return ["품목명이 비어 있음"]
    # 원문과 품목명의 문자 체계가 다르면 대조가 성립하지 않는다. 영어 원문에
    # 한글 음역("Souk"→"소우크", "dates"→"대추야자")이 오면 절대 안 맞아 전량 오탐이다.
    # number_problems 와 같은 가드다. 이 경우 판정은 아래 LLM 에 맡긴다.
    ko = lambda t: bool(re.search(r"[가-힣]", t or ""))
    if ko(item) != ko(d["body"]):
        return []
    norm = lambda t: re.sub(r"\s+", "", t or "").lower()
    return [] if norm(item) in norm(d["body"]) else [f"원문에 없는 품목명: {item}"]


def country_problems(d):
    """원문이 오늘의 국가 이야기인지. 국가명(한/영) 언급으로 대조한다.

    item 대조와 같은 방식이라 비용 0에 오탐이 없다.
    대신 본문이 도시명만 쓰고 국가명을 안 쓰면 놓친다 — 그건 위 LLM 판정이 받는다.
    """
    norm = lambda s: re.sub(r"\s+", "", s or "").lower()
    body = norm(d.get("body"))
    if norm(NATION["ko"]) in body or norm(NATION["en"]) in body:
        return []
    return [f"오늘의 국가({NATION['ko']}) 언급 없음"]


def verify(s: dict) -> dict:
    kept, dropped = [], []
    for d in s["drafted"]:
        # country_problems 는 SYS_CHECK 2번 기준과 중복이면서 본문이 도시명만 쓰면
        # 오탐을 낸다("두바이"만 적힌 UAE 기사). 국가 판정은 LLM 쪽에 일원화한다.
        problems = item_problems(d) or number_problems(d)
        if problems:
            dropped.append((d, problems))
            continue
        v = check(d)                           # 통과한 것만 LLM 에 묻는다
        (kept.append(d) if v.ok else dropped.append((d, v.problems)))
    return {"verified": kept,
            "log": [f"③ 취재   {len(s['picked'])} → {len(s['drafted'])}건",
                    f"④ 검수   {len(s['drafted'])} → {len(kept)}건"
                    + (f" · 불합격 {[x[0]['source'] for x in dropped]}" if dropped else "")]
                   + [f"   불합격 사유 [{d['source']}] {p[:60]}"
                      for d, probs in dropped for p in probs[:2]]}


# ───────────────────────── ⑤ 발행 ─────────────────────────
COLORS = {t["이름"]: t["색"] for t in CFG.get("토픽", [])}
DEFAULT = 0x5F7476
TITLE_MAX, DESC_MAX, EMBED_MAX, TOTAL_MAX = 256, 4096, 10, 5800   # 6000에서 여유를 둔다


CADENCE = CFG["발행"].get("주기_문구", "")
EMPTY   = CFG["발행"].get("없을때_문구", "오늘은 조용합니다.")


def build_embeds(run_id, lead, articles):
    # 한 건도 없는 주에도 한 장은 보낸다. 아무것도 안 보내면
    # 파이프라인이 죽은 것과 구분이 안 된다.
    head = {"title": f"🗞️ {run_id} · 🌎 {BLOC['대륙']} · {NATION['ko']}", "color": DEFAULT,
            "description": (lead if articles else EMPTY)
                           + (f"\n\n{CADENCE}" if CADENCE else "")}
    if not articles:
        return [head]
    embeds = [head]
    for i, a in enumerate(articles, 1):
        desc = a["summary"]
        if a.get("why"):
            desc += f"\n\n💡 **{a['why']}**"
        embeds.append({
            "title":       f"{i}. {a['headline']}"[:TITLE_MAX],
            "description": desc[:DESC_MAX],
            "url":         a["url"],
            "color":       COLORS.get(a.get("topic", ""), DEFAULT),
            "footer":      {"text": f"{a['source']} · {a['when']}" + (" · 날짜 미확인" if a.get("date_est") else "")},
        })
    total = lambda es: sum(len(e.get("title", "")) + len(e.get("description", ""))
                           + len(e.get("footer", {}).get("text", "")) for e in es)
    while len(embeds) > EMBED_MAX or total(embeds) > TOTAL_MAX:
        embeds.pop()                                   # 뒤에서부터 덜어낸다
    return embeds


def send(run_id, lead, articles, webhook=None, dry_run=True):
    payload = {"username": CFG["발행"]["이름"],
               "embeds": build_embeds(run_id, lead, articles)}
    if dry_run or not webhook:
        print(f"[dry-run] embed {len(payload['embeds'])}개 · "
              f"{len(json.dumps(payload, ensure_ascii=False))}자 — 보내지 않음")
        return False
    r = requests.post(webhook, json=payload, timeout=20)
    ok = r.status_code in (200, 204)
    print("발행:", "성공" if ok else f"실패 {r.status_code} {r.text[:120]}")
    return ok


def make_lead(arts):
    if not arts:
        return ""
    srcs = ", ".join(dict.fromkeys(a["source"] for a in arts))
    return f"{CFG['발행'].get('머리말_단위', '이번 주')} {len(arts)}건을 골랐습니다. ({srcs})"


def publish(s: dict) -> dict:
    # State 에는 body 처럼 발행에 쓰지 않는 칸이 있다. 필요한 칸만 골라 새로 만든다.
    arts = [{"headline": a["headline"], "summary": a["summary"], "why": a["why"],
             "url": a["url"], "source": a["source"], "topic": a.get("event", ""),
             "when": a["at"].strftime("%m-%d %H:%M"), "date_est": a.get("date_est")}
            for a in s["verified"][:TARGET]]   # 여유분을 두고 뽑았으므로 여기서 자른다
    today = datetime.now().strftime("%Y-%m-%d")
    sent  = send(today, make_lead(arts), arts,
                 webhook=os.environ.get("DISCORD_WEBHOOK_URL"),
                 dry_run=os.environ.get("DRY_RUN", "1") == "1")   # 기본은 보내지 않음
    if sent:                                   # 보낸 것만 기억한다. dry-run 은 기억하지 않는다
        save_seen(load_seen() | {canonical(a["url"]) for a in s["verified"]})
    label = f"{len(arts)}건" if arts else "조용합니다"
    return {"log": [f"⑤ 발행   {label} · {'보냄' if sent else 'dry-run'}"]}


# ───────────────────────── 그래프 ─────────────────────────
def build():
    g = StateGraph(Brief)
    for name in ("collect", "select", "report", "verify", "publish"):
        g.add_node(name, globals()[name])
    g.add_edge(START, "collect")
    g.add_edge("collect", "select")
    g.add_conditional_edges("select", fan_report, ["report"])   # 팬아웃 경계
    g.add_edge("report", "verify")
    g.add_edge("verify", "publish")
    g.add_edge("publish", END)
    return g


METRICS = pathlib.Path("store/metrics.jsonl")


def run(hours: int = None):
    """돌리고, 한 줄 남긴다.

    데이터베이스도 대시보드도 필요 없다. 파일 하나에 append.
    collected → picked → drafted → published 네 숫자가 깔때기다.
    """
    hours = hours or CFG["발행"]["시간창_시간"]
    out = build().compile().invoke({**INIT, "hours": hours})
    row = {"run_id":    datetime.now().strftime("%Y-%m-%d %H:%M"),
           "collected": len(out["collected"]),
           "picked":    len(out["picked"]),
           "drafted":   len(out["drafted"]),
           "published": len(out["verified"]),
           "hours":     out["hours"],
           "dry_run":   os.environ.get("DRY_RUN", "1") == "1",
           "by_source": Counter(a["source"] for a in out["verified"]),
           "log":       out["log"]}
    METRICS.parent.mkdir(exist_ok=True)
    with METRICS.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return out

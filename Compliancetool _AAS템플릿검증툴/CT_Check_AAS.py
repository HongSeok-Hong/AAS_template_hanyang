#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AAS 통합 검증 도구 (번역 + idShort 추적 + SMC/SME 컨텍스트)

- ts: UNIX epoch(초)
- smeIdShort: string | string[] (idShort가 리스트면 전체 유지)
- AASX: .rels TargetMode="External" 제거 후 검증
- CT(Compliance Tool), TE(Test Engines) 결과 결합
- 메시지 번역/정리 + 위치 추정(JSON Pointer, lxml 라인 근접)
- 인라인 대형 JSON/dict/array 페이로드: 핵심 키만 스마트 축약({idShort=…, modelType=…}), 실패 시 {...}/[...]
"""

from __future__ import annotations
import json, re, sys, zipfile, tempfile, subprocess, xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Union
from datetime import datetime, timezone

# ---------- lxml (선택) ----------
try:
    from lxml import etree as LET  # type: ignore
    HAS_LXML = True
except Exception:
    HAS_LXML = False

REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

# =========================
# 공통 정규식 / 헬퍼
# =========================
ANSI_RE = re.compile(r"\x1b\[[0-9;]*[mK]")
STRIP_PATH_TAIL = re.compile(r'\s*@\s*[/\{].*$', re.IGNORECASE)
XML_NS_TAG = re.compile(r"\{[^}]+\}([A-Za-z0-9_.:-]+)")
IDX_PATH = re.compile(r"^/(assetAdministrationShells|submodels)/(\d+)(?:/|$)")
PATH_RE = re.compile(r"/(?:assetAdministrationShells|submodels)/\d+(?:/\S*)?")
CT_LINE_TAG = re.compile(r"(?i)(?P<what>\{[^}]+\}[A-Za-z0-9_:\-\.]+|[A-Za-z0-9_:\-\.]+)\s+on\s+line\s+(?P<ln>\d+)")
CT_LINE_ONLY = re.compile(r"(?i)on\s+line\s+(?P<ln>\d+)")
CAND_PAT = re.compile(
    r"(missing|fail|failed|not\s+ok|invalid|could\s+not|exception|required|unexpected|skipped|must have|has no data specification|embeddedDataSpecification|Failed to construct|ValueError|TypeError|KeyError|AttributeError|AssertionError)",
    re.IGNORECASE
)
EXC_LAST_LINE = re.compile(r"^\s*(\w+(?:Error|Exception))\s*:\s*(.+)$")

SME_TAGS = {
    "property","range","file","blob","referenceelement","multilanguageproperty",
    "annotatedrelationshipelement","relationshipelement","operation","entity",
    "basicevent","capability"
}

def strip_ansi(s: str) -> str:
    return ANSI_RE.sub("", s or "")

def strip_xml_ns_tags(text: str) -> str:
    return XML_NS_TAG.sub(r"\1", text or "")

def lnamel(tag: str) -> str:
    if not isinstance(tag, str):
        return ""
    return (tag.split("}", 1)[1] if "}" in tag else tag).lower()

def now_epoch_seconds() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def run_cmd(cmd: List[str]) -> Tuple[int, str, str]:
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    return p.returncode, p.stdout, p.stderr

def clean_for_translation(s: str) -> str:
    s = strip_ansi(s or "").strip()
    s = re.sub(r'^\s*(?:[-—">]+|\*+)?\s*(?:ERROR|WARNING|INFO|DEBUG|TRACE)\s*[:\-]?\s*', '', s, flags=re.IGNORECASE)
    s = re.sub(r'^\s*(?:->\s*)+', '', s, flags=re.IGNORECASE)
    s = re.sub(r'^\s*KeyError:\s*', '', s, flags=re.IGNORECASE)
    s = STRIP_PATH_TAIL.sub("", s)
    s = strip_xml_ns_tags(s)
    return re.sub(r"\s+", " ", s).strip()

# ----- 메시지 간소화/스마트 축약 -----
_MSG_PAYLOAD_SPLIT = re.compile(r"\s*>>>\s*{.*$", re.DOTALL)
_PTR_SUFFIX = re.compile(r"\s*\(추정 위치:[^)]+\)\s*$")

def _summarize_json_like_block(block: str) -> Optional[str]:
    """
    단일 인라인 dict/array 텍스트에서 핵심 필드만 요약 문자열 생성.
    입력은 JSON이 아니라 Python dict 스타일(single quotes)일 수 있음.
    추출 대상:
      - idShort (단일/리스트)
      - modelType
      - category
      - semanticId.type / keys[0].type/value 는 최대 1~2개 힌트
    실패 시 None
    """
    if not block:
        return None

    # idShort: 단일/리스트
    idshort_single = None
    idshort_list: Optional[List[str]] = None
    m = re.search(r"'idShort'\s*:\s*'([^']+)'|\"idShort\"\s*:\s*\"([^\"]+)\"", block)
    if m:
        idshort_single = (m.group(1) or m.group(2))
    else:
        m2 = re.search(r"'idShort'\s*:\s*\[([^\]]+)\]|\"idShort\"\s*:\s*\[([^\]]+)\]", block)
        if m2:
            inside = m2.group(1) or m2.group(2)
            idshort_list = [g1 or g2 for g1, g2 in re.findall(r"'([^']+)'|\"([^\"]+)\"", inside)] or None

    # modelType
    model_type = None
    m = re.search(r"'modelType'\s*:\s*'([^']+)'|\"modelType\"\s*:\s*\"([^\"]+)\"", block)
    if m:
        model_type = (m.group(1) or m.group(2))

    # category
    category = None
    m = re.search(r"'category'\s*:\s*'([^']+)'|\"category\"\s*:\s*\"([^\"]+)\"", block)
    if m:
        category = (m.group(1) or m.group(2))

    # semanticId.type
    sem_type = None
    m = re.search(r"'semanticId'\s*:\s*{\s*'type'\s*:\s*'([^']+)'|\"semanticId\"\s*:\s*{\s*\"type\"\s*:\s*\"([^\"]+)\"", block)
    if m:
        sem_type = (m.group(1) or m.group(2))

    # semanticId.keys[0].value (힌트)
    sem_val = None
    m = re.search(r"'keys'\s*:\s*\[\s*{\s*'type'\s*:\s*'[^']*'\s*,\s*'value'\s*:\s*'([^']+)'", block)
    if not m:
        m = re.search(r"\"keys\"\s*:\s*\[\s*{\s*\"type\"\s*:\s*\"[^\"]*\"\s*,\s*\"value\"\s*:\s*\"([^\"]+)\"", block)
    if m:
        sem_val = m.group(1)

    # 구성
    parts = []
    if idshort_single:
        parts.append(f"idShort={idshort_single}")
    if idshort_list:
        parts.append("idShort=[" + ", ".join(idshort_list[:5]) + ("]" if len(idshort_list) <= 5 else ", …]"))
    if model_type:
        parts.append(f"modelType={model_type}")
    if category:
        parts.append(f"category={category}")
    if sem_type or sem_val:
        sem_bits = []
        if sem_type: sem_bits.append(f"type={sem_type}")
        if sem_val: sem_bits.append(f"value={sem_val}")
        parts.append("semanticId{" + ", ".join(sem_bits) + "}")

    if not parts:
        return None
    return "{" + ", ".join(parts) + "}"

def _shrink_json_like_blocks(text: str, min_len: int = 80) -> str:
    """
    문장 안에 포함된 거대한 {...} / [...] 블록을 스마트 요약으로 대체:
    - dict/array 블록 파싱 → 핵심 키만 요약
    - 실패 시 {...} / [...] 로 축약
    - 중첩 괄호를 스택으로 처리
    """
    if not text:
        return text

    s = text
    out = []
    i = 0
    n = len(s)

    while i < n:
        ch = s[i]
        if ch in "{[":
            open_ch = ch
            close_ch = "}" if ch == "{" else "]"
            depth = 0
            j = i
            while j < n:
                cj = s[j]
                if cj == open_ch:
                    depth += 1
                elif cj == close_ch:
                    depth -= 1
                    if depth == 0:
                        break
                j += 1

            if j >= n:
                out.append(ch)
                i += 1
                continue

            block = s[i : j + 1]
            if len(block) >= min_len:
                if open_ch == "{":
                    summary = _summarize_json_like_block(block)
                    out.append(summary if summary else "{...}")
                else:
                    out.append("[...]")
            else:
                out.append(block)
            i = j + 1
        else:
            out.append(ch)
            i += 1

    return "".join(out)

def simplify_msg_keep_location(msg: str) -> str:
    """
    1) (추정 위치: ...) 꼬리는 보존
    2) '>>> { ... }' 스타일 덤프는 제거
    3) 문장 중간의 큰 JSON/dict/array 블록은 스마트 요약(핵심 키) 또는 {...}/[...]로 축약
    """
    if not msg:
        return msg

    # (1) 추정 위치 꼬리를 분리해 보존
    loc_tail = None
    m = _PTR_SUFFIX.search(msg)
    core = msg
    if m:
        loc_tail = m.group(0)
        core = msg[:m.start()]

    # (2) >>> { ... } 덤프 제거
    core = _MSG_PAYLOAD_SPLIT.sub("", core).rstrip()

    # (3) 인라인 대형 JSON-like 블록 스마트 축약
    core = _shrink_json_like_blocks(core, min_len=80).rstrip()

    # (4) 꼬리 복원
    if loc_tail:
        return f"{core}{loc_tail}"
    return core

# =========================
# 번역 규칙 (간결화)
# =========================
SPEC_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r'^Check meta model$', re.I), '메타모델 검사'),
    (re.compile(r'^Check$', re.I), '검사'),
    (re.compile(r'^Check constraints$', re.I), '제약 검사'),
    (re.compile(r'^Checking files$', re.I), '파일 검사'),
    (re.compile(r'^Checking relationships$', re.I), '관계 검사'),
    (re.compile(r'^Checking root relationship$', re.I), '루트 관계 검사'),
    (re.compile(r'^Checking content types$', re.I), '콘텐츠 타입 검사'),
    (re.compile(r'^Skipped checking of constraints$', re.I), '제약 검사를 건너뜀'),
    (re.compile(r'^Check OPC package$', re.I), 'OPC 패키지 검사'),
    (re.compile(r'^Check AASX package$', re.I), 'AASX 패키지 검사'),
    (re.compile(r'^Unsupported type: (.+)$', re.I), r'지원되지 않는 타입: \1'),
    (re.compile(r'^Failed to reach: (.*)$', re.I), r'접속 실패: \1'),
    (re.compile(r'^Skipped due to dry run$', re.I), r'드라이 런으로 인해 건너뜀'),
]

GEN_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"Failed:\s*Read\s+file\s+and\s+check\s+if\s+it\s+is\s+deserializable", re.I),
     r"파일 읽기 및 역직렬화 가능성 검사 실패"),
    (re.compile(r"Failed:\s*Read\s+file", re.I), r"파일 읽기 실패"),
    (re.compile(r"Failed:\s*(.+)", re.I), r"실패: \1"),
    (re.compile(r"ValueError:\s*\{https://admin-shell\.io/aas/3/0\}reference\s+on\s+line\s+(\d+)\s+is\s+of\s+type\s+<class\s+'([^']+)'>,?\s*expected\s+<class\s+'([^']+)'>!", re.I),
     r"\1행: reference가 \2 타입이지만 \3 타입이 예상됩니다"),
    (re.compile(r"ValueError:\s*Expected\s+a\s+reference\s+of\s+type\s+<class\s+'([^']+)'>,\s*got\s*<class\s+'([^']+)'>!?", re.I),
     r"reference가 \2 타입이지만 \1 타입이 예상됩니다"),
    (re.compile(r"Error\s+while\s+trying\s+to\s+convert\s+JSON\s+object\s+into\s+([^:]+):\s*(.+)", re.I),
     r"JSON을 \1로 변환하는 중 오류: \2"),
    (re.compile(r"Expected\s+(.+?),\s+but\s+found\s+(.+)", re.I),
     r"\1이(가) 예상되었지만 \2을(를) 발견했습니다"),
    (re.compile(r'\bConstraint\s+([A-Za-z0-9\-]+)\s+(?:is\s+)?violated:\s*', re.I), r'제약 \1 위반: '),
    (re.compile(r'\bon\s+line\s+(\d+)\b', re.I), r'\1행'),
]

def translate_message(text: str) -> str:
    base = clean_for_translation(text)
    m = re.match(r'^([A-Za-z0-9_.:-]+)\s+on\s+line\s+(\d+)\s+has\s+no\s+data\s+specification!?$', base, re.IGNORECASE)
    if m:
        tag, ln = m.group(1), m.group(2)
        return f"{ln}행: {tag}에 DataSpecification이 없습니다."
    m = re.search(r'Failed\s+to\s+construct\s+([A-Za-z0-9_.:-]+)\s+on\s+line\s+(\d+)\s+using\s+([A-Za-z0-9_.:-]+)', base, re.IGNORECASE)
    if m:
        x, ln, y = m.group(1), m.group(2), m.group(3)
        return f"{ln}행: {y}를 사용해 {x}를 구성하지 못했습니다."
    for pat, repl in SPEC_RULES + GEN_RULES:
        if pat.search(base):
            return pat.sub(repl, base).strip()
    out = base
    out = re.sub(r'\bhas\s+no\s+data\s+specification!?', 'DataSpecification이 없습니다', out, flags=re.IGNORECASE)
    return re.sub(r'\s{2,}', ' ', out).strip()

# =========================
# AASX 전처리
# =========================
def _strip_external_in_rels_xml(xml_bytes: bytes) -> Tuple[bytes, int]:
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return xml_bytes, 0
    removed = 0
    for rel in root.findall(f"{{{REL_NS}}}Relationship"):
        if (rel.attrib.get("TargetMode") or "").strip().lower() == "external":
            del rel.attrib["TargetMode"]; removed += 1
    return (ET.tostring(root, encoding="utf-8", xml_declaration=True), removed) if removed else (xml_bytes, 0)

def patch_aasx_only_external_strip(src: Path) -> Path:
    tmp = tempfile.NamedTemporaryFile(suffix=".aasx", delete=False); tmp.close()
    out = Path(tmp.name)
    with zipfile.ZipFile(src, "r") as zin, zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
        for info in zin.infolist():
            data = zin.read(info.filename)
            if info.filename.endswith(".rels"):
                data, _ = _strip_external_in_rels_xml(data)
            zout.writestr(info, data)
    return out

# =========================
# 입력 로딩/ID 수집
# =========================
def _append_if_text_id(lst: List[str], node: ET.Element, tags=("id","identification")):
    for ch in node.iter():
        ln2 = lnamel(ch.tag)
        if ln2 in tags and ch.text and ch.text.strip():
            lst.append(ch.text.strip()); return

def load_source_maps(in_path: Path) -> Dict[str, Any]:
    info: Dict[str, Any] = {"type": in_path.suffix.lower().lstrip("."), "json_docs": [], "lx_docs": [], "et_docs": [], "aas_ids": [], "sm_ids": []}

    def add_ids_from_json(obj: Dict[str, Any]):
        for a in (obj.get("assetAdministrationShells") or []):
            if isinstance(a, dict):
                info["aas_ids"].append((a.get("id") or a.get("identification") or a.get("idShort") or "").strip())
        for s in (obj.get("submodels") or []):
            if isinstance(s, dict):
                info["sm_ids"].append((s.get("id") or s.get("identification") or s.get("idShort") or "").strip())

    try:
        t = info["type"]
        if t == "json":
            with open(in_path, "r", encoding="utf-8") as f: j = json.load(f)
            info["json_docs"].append(j); add_ids_from_json(j)
        elif t == "xml":
            if HAS_LXML:
                with open(in_path, "rb") as f: b = f.read()
                lx = LET.fromstring(b, parser=LET.XMLParser(remove_blank_text=False, huge_tree=True, recover=True)).getroottree()
                info["lx_docs"].append({"name": in_path.name, "tree": lx})
                root = lx.getroot()
                for e in root.iter():
                    ln = lnamel(e.tag)
                    if ln == "assetadministrationshell": _append_if_text_id(info["aas_ids"], e)
                    elif ln == "submodel": _append_if_text_id(info["sm_ids"], e)
            root = ET.parse(in_path).getroot()
            info["et_docs"].append({"name": in_path.name, "root": root})
            for e in root.iter():
                ln = lnamel(e.tag)
                if ln == "assetadministrationshell": _append_if_text_id(info["aas_ids"], e)
                elif ln == "submodel": _append_if_text_id(info["sm_ids"], e)
        elif t == "aasx":
            with zipfile.ZipFile(in_path, "r") as z:
                for name in [n for n in z.namelist() if n.lower().startswith("aasx/") and n.lower().endswith(".xml")]:
                    data = z.read(name)
                    if HAS_LXML:
                        try:
                            lx = LET.fromstring(data, parser=LET.XMLParser(remove_blank_text=False, huge_tree=True, recover=True)).getroottree()
                            info["lx_docs"].append({"name": name, "tree": lx})
                            root = lx.getroot()
                            for e in root.iter():
                                ln = lnamel(e.tag)
                                if ln == "assetadministrationshell": _append_if_text_id(info["aas_ids"], e)
                                elif ln == "submodel": _append_if_text_id(info["sm_ids"], e)
                        except Exception:
                            pass
                    try:
                        root = ET.fromstring(data)
                        info["et_docs"].append({"name": name, "root": root})
                        for e in root.iter():
                            ln = lnamel(e.tag)
                            if ln == "assetadministrationshell": _append_if_text_id(info["aas_ids"], e)
                            elif ln == "submodel": _append_if_text_id(info["sm_ids"], e)
                    except Exception:
                        pass
                for name in [n for n in z.namelist() if n.lower().startswith("aasx/") and n.lower().endswith(".json")]:
                    try:
                        j = json.loads(z.read(name).decode("utf-8", errors="replace"))
                        info["json_docs"].append(j); add_ids_from_json(j)
                    except Exception:
                        pass
    except Exception:
        pass

    info["aas_ids"] = [x for x in dict.fromkeys([x for x in info["aas_ids"] if x])]
    info["sm_ids"]  = [x for x in dict.fromkeys([x for x in info["sm_ids"] if x])]
    return info

def idshort_by_id(source_info: Dict[str, Any], ident: Optional[str]) -> Optional[str]:
    if not ident: return None
    def scan(o):
        if isinstance(o, dict):
            if (o.get("id") or o.get("identification")) == ident:
                ids = o.get("idShort")
                if isinstance(ids, str) and ids.strip():
                    return ids.strip()
            for v in o.values():
                r = scan(v)
                if r: return r
        elif isinstance(o, list):
            for it in o:
                r = scan(it)
                if r: return r
        return None
    for j in source_info.get("json_docs") or []:
        r = scan(j)
        if r: return r
    if HAS_LXML:
        for doc in source_info.get("lx_docs") or []:
            try:
                root = doc["tree"].getroot()
                for e in root.iter():
                    if lnamel(e.tag) in ("id","identification") and e.text and e.text.strip() == ident:
                        cur = e.getparent()
                        while cur is not None:
                            for ch in cur.iter():
                                if lnamel(ch.tag).endswith("idshort") and ch.text and ch.text.strip():
                                    return ch.text.strip()
                            cur = cur.getparent()
            except Exception: pass
    for doc in source_info.get("et_docs") or []:
        try:
            root: ET.Element = doc["root"]
            parent_map = {c: p for p in root.iter() for c in list(p)}
            for e in root.iter():
                if lnamel(e.tag) in ("id","identification") and e.text and e.text.strip() == ident:
                    cur = parent_map.get(e)
                    while cur is not None:
                        for ch in cur.iter():
                            if lnamel(ch.tag).endswith("idshort") and ch.text and ch.text.strip():
                                return ch.text.strip()
                        cur = parent_map.get(cur)
        except Exception: pass
    return None

# =========================
# JSON Pointer / 경로 해석
# =========================
def ids_from_json_pointer(doc: Any, pointer: str) -> Tuple[Optional[str], Optional[str]]:
    parts = [p for p in pointer.split("/") if p]
    cur = doc; parents = []
    for p in parts:
        parents.append(cur)
        if isinstance(cur, list):
            try: cur = cur[int(p)]
            except Exception: break
        elif isinstance(cur, dict):
            cur = cur.get(p)
        else:
            break
    chain = [cur] + parents[::-1]
    id_val = id_short = None
    for node in chain:
        if isinstance(node, dict):
            id_val   = id_val   or node.get("id") or node.get("identification")
            id_short = id_short or node.get("idShort")
            if id_val and id_short: break
    return id_val, id_short

def extract_te_path(line: str) -> Optional[str]:
    m = PATH_RE.search(line or "");  return m.group(0) if m else None

def ids_from_te_path(source_info: Dict[str, Any], path: Optional[str], in_path: Path) -> Tuple[str, Optional[str]]:
    fallback_id = (source_info.get("aas_ids") or source_info.get("sm_ids") or [f"{in_path.stem}::global"])[0]
    found_id = found_short = None
    if path:
        m = IDX_PATH.match(path.strip())
        if m:
            kind, idx = m.group(1), int(m.group(2))
            ids = (source_info.get("aas_ids") if kind == "assetAdministrationShells" else source_info.get("sm_ids")) or []
            if 0 <= idx < len(ids) and ids[idx]:
                found_id = ids[idx]
        for jdoc in source_info.get("json_docs") or []:
            try:
                id_val, id_short = ids_from_json_pointer(jdoc, path)
                found_id   = found_id   or id_val
                found_short = found_short or id_short
                if found_id and found_short: break
            except Exception: pass
    refer_id = found_id or fallback_id
    return refer_id, (found_short or idshort_by_id(source_info, refer_id))

# =========================
# 메시지 → SMC/SME 후보 추출
# =========================
def parse_string_list(text_inside_brackets: str) -> List[str]:
    return [m.group(1) or m.group(2) for m in re.finditer(r"'([^']+)'|\"([^\"]+)\"", text_inside_brackets)]

def extract_element_info_from_message(msg_line: str) -> Tuple[Optional[str], Optional[Union[str, List[str]]]]:
    m = re.search(r'\bSubmodelElement\[(?P<s>[^\]]+)\]', msg_line, re.I)
    if m: return "SubmodelElement", m.group("s")
    m = re.search(r'\bin\s+SubmodelElement\[(?P<s>[^\]]+)\]', msg_line, re.I)
    if m: return "SubmodelElement", m.group("s")
    m = re.search(r'\bSubmodelElementCollection\[(?P<s>[^\]]+)\]', msg_line, re.I)
    if m: return "SubmodelElementCollection", m.group("s")
    m = re.search(r"SubmodelElement\s+idShort\s*=\s*'([^']+)'", msg_line, re.I)
    if m: return "SubmodelElement", m.group(1)
    m = re.search(r"'idShort'\s*:\s*'([^']+)'", msg_line) or re.search(r'idShort"\s*:\s*"([^"]+)"', msg_line)
    if m: return "element", m.group(1)
    m = re.search(r"idShort'\s*:\s*\[([^\]]+)\]", msg_line) or re.search(r'idShort"\s*:\s*\[([^\]]+)\]', msg_line)
    if m:
        vals = parse_string_list(m.group(1))
        if vals: return "element_list", vals
    return None, None

def _format_ctx(smc: Optional[str], sme: Optional[Union[str, List[str]]]) -> Optional[Union[str, List[str]]]:
    smc = (smc or "").strip() or None
    if sme is None:
        return smc
    if isinstance(sme, list):
        return [f"{smc}.{s}" if smc else s for s in sme] if sme else smc
    return f"{smc}.{sme}" if smc else sme

# JSON에서 경로 기반 SMC/SME 얻기
def _resolve_json_pointer_with_stack(doc: Any, pointer: str):
    parts = [p for p in pointer.split("/") if p]
    cur = doc; stack = []
    for p in parts:
        stack.append(cur)
        if isinstance(cur, list):
            try: cur = cur[int(p)]
            except Exception: return None, stack
        elif isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return None, stack
    return cur, stack

def _get_model_type(node: Any) -> str:
    if isinstance(node, dict):
        mt = node.get("modelType")
        return (mt.get("name") or mt.get("modelType") or mt or "").strip() if isinstance(mt, dict) else (mt or "").strip()
    return ""

def json_ctx_by_path(source_info: Dict[str, Any], path: Optional[str]) -> Tuple[Optional[str], Optional[Union[str, List[str]]]]:
    if not path: return None, None
    for jdoc in source_info.get("json_docs") or []:
        node, stack = _resolve_json_pointer_with_stack(jdoc, path)
        if not isinstance(node, dict): continue
        smc = None; sme: Optional[Union[str, List[str]]] = None
        mt = _get_model_type(node)
        ids = node.get("idShort")
        if isinstance(ids, str) and ids.strip():
            if mt == "SubmodelElementCollection": smc = ids.strip()
            elif mt not in ("Submodel", "AssetAdministrationShell"): sme = ids.strip()
        elif isinstance(ids, list) and ids:
            if mt not in ("Submodel", "AssetAdministrationShell"):
                sme = [str(x) for x in ids if isinstance(x, (str,int,float))]
        for parent in reversed(stack):
            if isinstance(parent, dict) and not smc and _get_model_type(parent) == "SubmodelElementCollection":
                sid = parent.get("idShort")
                if isinstance(sid, str) and sid.strip():
                    smc = sid.strip(); break
        if smc or sme: return smc, sme
    return None, None

# =========================
# 위치 힌트 보강
# =========================
def _find_external_ref_paths_in_aas_submodels(jdoc: Dict[str, Any]) -> List[str]:
    paths: List[str] = []
    for i, aas in enumerate((jdoc.get("assetAdministrationShells") or [])):
        if not isinstance(aas, dict): continue
        for j, ref in enumerate((aas.get("submodels") or [])):
            if not isinstance(ref, dict): continue
            t = (ref.get("type") or ref.get("modelType") or "").strip()
            keys = ref.get("keys") or []
            is_ext = (t == "ExternalReference") or any(isinstance(k, dict) and (k.get("type") == "GlobalReference") for k in keys)
            if is_ext: paths.append(f"/assetAdministrationShells/{i}/submodels/{j}")
    return paths

def augment_modelref_hint(msg_ko: str, src_info: Dict[str, Any]) -> str:
    if not ("ModelReference" in msg_ko and "ExternalReference" in msg_ko): return msg_ko
    hints: List[str] = []
    for j in src_info.get("json_docs") or []:
        try: hints.extend(_find_external_ref_paths_in_aas_submodels(j))
        except Exception: pass
    hints = list(dict.fromkeys(hints))
    return f"{msg_ko} (추정 위치: {', '.join(hints)})" if hints else msg_ko

def scan_idshort_list_nodes_with_context(jdoc: Dict[str, Any]) -> List[Tuple[str, List[str], Optional[str]]]:
    results: List[Tuple[str, List[str], Optional[str]]] = []
    def rec(node: Any, path: str, smc_stack: List[str]):
        mt = _get_model_type(node)
        if isinstance(node, dict):
            cur_smc_stack = smc_stack
            if mt == "SubmodelElementCollection":
                sid = node.get("idShort")
                if isinstance(sid, str) and sid.strip():
                    cur_smc_stack = smc_stack + [sid.strip()]
            ids = node.get("idShort")
            if isinstance(ids, list) and ids:
                parent_smc = cur_smc_stack[-1] if cur_smc_stack else None
                vals = [str(x) for x in ids if isinstance(x, (str,int,float))]
                p = path if path.startswith("/") else f"/{path}"
                results.append((p, vals, parent_smc))
            for k, v in node.items(): rec(v, f"{path}/{k}", cur_smc_stack)
        elif isinstance(node, list):
            for i, it in enumerate(node): rec(it, f"{path}/{i}", smc_stack)
    rec(jdoc, "", [])
    return results

def find_context_by_idshort_candidate(source_info: Dict[str, Any], candidate: str) -> Tuple[Optional[str], List[str]]:
    paths: List[str] = []; smc: Optional[str] = None
    for jdoc in source_info.get("json_docs") or []:
        try:
            for ptr, vals, parent_smc in scan_idshort_list_nodes_with_context(jdoc):
                if candidate in vals:
                    paths.append(ptr)
                    if parent_smc and not smc: smc = parent_smc
        except Exception: pass
    return smc, list(dict.fromkeys(paths))

# =========================
# 이슈 수집/등급 + Traceback 처리
# =========================
def collect_issue_lines(text: str) -> List[str]:
    out: List[str] = []; in_tb = False; tb_last: Optional[str] = None
    for raw in (text or "").splitlines() + ["__END__SENTINEL__"]:
        line = raw.rstrip("\n")
        if line.startswith("Traceback (most recent call last):"):
            if in_tb and tb_last: out.append(tb_last.strip())
            in_tb = True; tb_last = None; continue
        if in_tb:
            m = EXC_LAST_LINE.match(line)
            if m: tb_last = line.strip(); continue
            if line.strip().startswith('File "') or line.strip().startswith("^"): continue
            continue
        t = line.strip()
        if t and not (t.startswith('File "') or t.startswith("^")) and CAND_PAT.search(t):
            out.append(t)
    if in_tb and tb_last: out.append(tb_last.strip())
    return list(dict.fromkeys(out))

def level_of_line(line: str) -> int:
    s = (line or "").lower()
    if "skipped" in s: return -1
    if any(k in s for k in ["error","exception","fail","failed","invalid","could not","required",
                             "unexpected","must have","missing","has no data specification",
                             "embeddeddataspecification","valueerror","typeerror","keyerror",
                             "attributeerror","assertionerror"]):
        return -2
    return 0

# =========================
# 외부 도구 실행
# =========================
def run_ct(in_path: Path, typ: str) -> str:
    py = sys.executable
    cmd = [py, "-m", "aas_compliance_tool.cli", "deserialization", str(in_path)]
    if   typ == "json": cmd += ["--json"]
    elif typ == "xml":  cmd += ["--xml"]
    elif typ == "aasx-xml":  cmd += ["--xml", "--aasx"]
    elif typ == "aasx-json": cmd += ["--json", "--aasx"]
    cmd += ["-vv"]
    _, out, err = run_cmd(cmd)
    return (out or "") + ("\n" + err if err else "")

def run_te(in_path: Path, fmt: str) -> str:
    cmd = [sys.executable, "-m", "aas_test_engines", "check_file", str(in_path), "--format", fmt]
    _, out, err = run_cmd(cmd)
    return (out or "") + ("\n" + err if err else "")

def detect_aasx_payload_kind(aasx_path: Path) -> str:
    kind = "unknown"
    try:
        with zipfile.ZipFile(aasx_path, "r") as z:
            names = [n.lower() for n in z.namelist()]
            has_json = any(n.startswith("aasx/") and n.endswith(".json") for n in names)
            has_xml  = any(n.startswith("aasx/") and n.endswith(".xml")  for n in names)
            if has_json and not has_xml: return "json"
            if has_xml and not has_json: return "xml"
            if "[content_types].xml" in names:
                root = ET.fromstring(z.read("[Content_Types].xml"))
                xmls = ET.tostring(root, encoding="unicode").lower()
                if "asset-administration-shell+json" in xmls or "application/json" in xmls: kind = "json"
                if "asset-administration-shell+xml"  in xmls or "application/xml"  in xmls:
                    if kind != "json": kind = "xml"
    except Exception:
        pass
    return kind

# =========================
# CT/TE → 식별/컨텍스트 추정
# =========================
def ids_from_ct_hint(source_info: Dict[str, Any], msg_line: str, in_path: Path) -> Tuple[str, Optional[str]]:
    etype, element_idshort = extract_element_info_from_message(msg_line)
    key = (element_idshort[0] if isinstance(element_idshort, list) and element_idshort else element_idshort)
    if key:
        if etype in ("SubmodelElementCollection", "SubmodelElement", "element", "element_list"):
            pid, pshort = find_parent_submodel_by_element_idshort(source_info, key)
            if pid: return pid, (pshort or idshort_by_id(source_info, pid))
        did = find_id_by_idshort_in_source(source_info, key)
        if did: return did, idshort_by_id(source_info, did)

    if HAS_LXML and (source_info.get("lx_docs") or []):
        m = CT_LINE_TAG.search(msg_line); prefer_local = line = None
        if m:
            what = m.group("what"); prefer_local = (what.split("}",1)[1] if "}" in what else what).split(":")[-1]
            line = int(m.group("ln"))
        else:
            m2 = CT_LINE_ONLY.search(msg_line);  line = int(m2.group("ln")) if m2 else None
        if line is not None:
            best_choice = None; best_score = 10**9
            for doc in source_info.get("lx_docs") or []:
                try:
                    lx = doc["tree"]
                    cand = None; cand_score = 10**9
                    for e in lx.iter():
                        ln = getattr(e, "sourceline", None)
                        if isinstance(ln, int):
                            d = abs(line - ln)
                            if prefer_local and lnamel(e.tag) == prefer_local.lower(): d = max(0, d - 10**6)
                            if d < cand_score: cand, cand_score = e, d
                    if cand and cand_score < best_score: best_choice, best_score = cand, cand_score
                except Exception: pass
            if best_choice is not None:
                aas_id = aas_short = sub_id = sub_short = None
                cur = best_choice
                while cur is not None:
                    tl = lnamel(cur.tag)
                    if tl == "assetadministrationshell":
                        if not aas_id or not aas_short:
                            for ch in cur.iter():
                                l2 = lnamel(ch.tag)
                                if l2 in ("id","identification") and ch.text and ch.text.strip() and not aas_id: aas_id = ch.text.strip()
                                elif l2.endswith("idshort") and ch.text and ch.text.strip() and not aas_short: aas_short = ch.text.strip()
                                if aas_id and aas_short: break
                    elif tl == "submodel":
                        if not sub_id or not sub_short:
                            for ch in cur.iter():
                                l2 = lnamel(ch.tag)
                                if l2 in ("id","identification") and ch.text and ch.text.strip() and not sub_id: sub_id = ch.text.strip()
                                elif l2.endswith("idshort") and ch.text and ch.text.strip() and not sub_short: sub_short = ch.text.strip()
                                if sub_id and sub_short: break
                    cur = cur.getparent() if hasattr(cur, "getparent") else None
                if sub_id: return sub_id, (sub_short or idshort_by_id(source_info, sub_id))
                if aas_id: return aas_id, (aas_short or idshort_by_id(source_info, aas_id))

    base = (source_info.get("aas_ids") or source_info.get("sm_ids") or [f"{in_path.stem}::global"])[0]
    return base, idshort_by_id(source_info, base)

def find_id_by_idshort_in_source(source_info: Dict[str, Any], target_idshort: str) -> Optional[str]:
    if not target_idshort: return None
    for jdoc in source_info.get("json_docs", []):
        def scan(o):
            if isinstance(o, dict):
                ids = o.get("idShort")
                if ids == target_idshort or (isinstance(ids, list) and target_idshort in ids):
                    return o.get("id") or o.get("identification")
                for v in o.values():
                    r = scan(v)
                    if r: return r
            elif isinstance(o, list):
                for it in o:
                    r = scan(it)
                    if r: return r
            return None
        r = scan(jdoc)
        if r: return r
    return None

def find_parent_submodel_by_element_idshort(source_info: Dict[str, Any], element_idshort: str) -> Tuple[Optional[str], Optional[str]]:
    for jdoc in source_info.get("json_docs", []):
        def walk(o, parent=None):
            if isinstance(o, dict):
                if _get_model_type(o) == "Submodel":
                    parent = (o.get("id") or o.get("identification"), o.get("idShort"))
                ids = o.get("idShort")
                if ids == element_idshort or (isinstance(ids, list) and element_idshort in ids):
                    return parent
                for v in o.values():
                    r = walk(v, parent)
                    if r: return r
            elif isinstance(o, list):
                for it in o:
                    r = walk(it, parent)
                    if r: return r
            return None
        r = walk(jdoc)
        if r: return r
    return None, None

# =========================
# 메인
# =========================
def main():
    include_src = False
    args = list(sys.argv[1:])
    if not args or (args[0] in ("-h","--help")):
        print("Usage: python aas_unified_check.py [--debug] <path-to-json|xml|aasx>"); sys.exit(2)
    if args[0] == "--debug": include_src = True; args = args[1:]
    if len(args) != 1:
        print("Usage: python aas_unified_check.py [--debug] <path-to-json|xml|aasx>"); sys.exit(2)

    in_path = Path(args[0]).resolve()
    if not in_path.exists(): print(f"ERROR: file not found: {in_path}"); sys.exit(2)
    kind = in_path.suffix.lower().lstrip(".")
    if kind not in ("json","xml","aasx"): print("ERROR: unsupported input type"); sys.exit(2)

    print("="*60); print("AAS 통합 검증 도구 (번역 + idShort 추적 + SMC/SME 컨텍스트)"); print("="*60)
    print("AAS 용어: 영어 원문 유지"); print("="*60)

    print(f"📂 파일 로딩: {in_path.name}")
    src_info = load_source_maps(in_path)
    print(f"   - AAS IDs: {len(src_info.get('aas_ids', []))}개")
    print(f"   - Submodel IDs: {len(src_info.get('sm_ids', []))}개")

    work = in_path; tmp_files: List[Path] = []
    if kind == "aasx":
        print("🔧 AASX 전처리 중...(rels의 TargetMode=External 제거)")
        work = patch_aasx_only_external_strip(in_path); tmp_files.append(work)

    print("\n🧪 검증 실행 중...")

    # 1) CT
    print("   - CT (Compliance Tool) 실행...")
    if kind == "json": ct_raw = run_ct(work, "json")
    elif kind == "xml": ct_raw = run_ct(work, "xml")
    else:
        det = detect_aasx_payload_kind(work)
        first, second = ("aasx-xml","aasx-json") if det in ("xml","unknown") else ("aasx-json","aasx-xml")
        print(f"     (AASX 페이로드: {det}, {first} -> {second} 순서)")
        a = run_ct(work, first); b = run_ct(work, second)
        ct_raw = "\n".join(list(dict.fromkeys(collect_issue_lines(a)+collect_issue_lines(b)))) or a

    ct_issues: List[Dict[str, Any]] = []
    for ln in collect_issue_lines(ct_raw):
        lvl = level_of_line(ln)
        if lvl == 0: continue
        ident, idshort = ids_from_ct_hint(src_info, ln, in_path)
        msg_ko = translate_message(ln)
        msg_ko = augment_modelref_hint(msg_ko, src_info)
        smc_msg, sme_msg = _msg_ctx(ln)
        # idShort 리스트 오류 위치 힌트(리스트면 첫 요소로 위치 추정, 출력은 리스트 유지)
        if sme_msg is None or (isinstance(sme_msg, list) and not sme_msg):
            etype, sme_cand = extract_element_info_from_message(ln)
            probe = sme_cand[0] if isinstance(sme_cand, list) and sme_cand else sme_cand
            if probe:
                smc_hint, paths = find_context_by_idshort_candidate(src_info, probe)
                if paths: msg_ko = f"{msg_ko} (추정 위치: {', '.join(paths[:3])})"
                if not smc_msg and smc_hint: smc_msg = smc_hint
            if sme_msg is None and isinstance(sme_cand, list) and sme_cand:
                sme_msg = sme_cand
        # --- 스마트 축약 적용(라인/추정위치 보존)
        msg_ko = simplify_msg_keep_location(msg_ko)

        ctx_val = _format_ctx(smc_msg, sme_msg)
        item = {"level": lvl, "id": ident, "idShort": idshort, "smeIdShort": ctx_val, "msg": msg_ko}
        if include_src: item["src"] = "CT"
        ct_issues.append(item)

    ct_has_error = any(i["level"] == -2 for i in ct_issues)

    # 2) TE 단계는 CT 전용 버전에서는 수행하지 않음
    te_issues: List[Dict[str, Any]] = []
    # 이 스크립트에서는 aas_test_engines (TE)를 실행하지 않고,
    # basyx.aas.compliance_tool (CT) 결과만 사용합니다.

    issues = ct_issues + te_issues
    rsc = -2 if any(i["level"] == -2 for i in issues) else (-1 if any(i["level"] == -1 for i in issues) else 0)

    print("\n" + "="*60); print("📊 검증 결과 요약"); print("="*60)
    ec = sum(1 for i in issues if i["level"] == -2); wc = sum(1 for i in issues if i["level"] == -1)
    print("✅ 검증 성공 - 이슈 없음" if rsc == 0 else (f"⚠️ 경고 발견 - {wc}개 경고" if rsc == -1 else f"❌ 오류 발견 - {ec}개 오류, {wc}개 경고"))
    print(f"   총 이슈: {len(issues)}개 | CT: {len(ct_issues)} | TE: {len(te_issues)}")

    out = {"ts": now_epoch_seconds(), "rsc": rsc, "rsn": issues}
    out_path = in_path.with_name(f"{in_path.with_suffix('').name}__ko_report.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"\n💾 결과 저장: {out_path}")
    print("="*60)

    for p in tmp_files:
        try:
            if p.exists(): p.unlink()
        except Exception:
            pass

# 메시지 → (SMC, SME) 단서 추출(리스트 지원)
def _msg_ctx(line: str) -> Tuple[Optional[str], Optional[Union[str, List[str]]]]:
    smc = None; sme: Optional[Union[str, List[str]]] = None
    m = re.search(r'\bSubmodelElementCollection\[(?P<s>[^\]]+)\]', line, re.I)
    if m: smc = m.group("s").strip()
    m = re.search(r'\bSubmodelElement\[(?P<s>[^\]]+)\]', line, re.I)
    if m: sme = m.group("s").strip()
    if (sme is None) and (re.search(r'\bSubmodelElement\b', line, re.I) or re.search(r'\bSubmodelElementCollection\b', line, re.I)):
        m = re.search(r"'idShort'\s*:\s*'([^']+)'", line) or re.search(r'idShort"\s*:\s*"([^"]+)"', line)
        if m:
            sme = m.group(1).strip()
        else:
            m = re.search(r"idShort'\s*:\s*\[([^\]]+)\]", line) or re.search(r'idShort"\s*:\s*\[([^\]]+)\]', line)
            if m:
                vals = parse_string_list(m.group(1))
                if vals: sme = vals
    return smc, sme

if __name__ == "__main__":
    main()

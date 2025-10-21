import asyncio
import re
import unicodedata
from typing import List, Set, Tuple

from pydantic import BaseModel, Field

from agentics import AG
from pathlib import Path

DATA_DIR = Path("./data/")
OUTPUT_DIR = Path(DATA_DIR) / "outputs/"

# ---------- Helpers: normalization ----------

_NUM_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")


def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", s) if unicodedata.category(c) != "Mn"
    )


def _remove_parentheticals(s: str) -> str:
    # Remove (...) segments
    return re.sub(r"\([^)]*\)", "", s)


def _basic_norm(x) -> str:
    """
    Basic content normalization:
    - stringify, lowercase, strip
    - remove accents
    - remove parentheticals
    """
    if x is None:
        return ""
    if isinstance(x, list):
        x = ", ".join(map(str, x))
    s = str(x).strip().lower()
    s = _strip_accents(s)
    s = _remove_parentheticals(s)
    s = s.strip()
    return s


def _collapse_spaces_and_punct(s: str) -> str:
    """
    Keep: alnum, spaces, dot, dash, comma, slash, percent.
    Collapse multi-spaces.
    """
    s = re.sub(r"[^\w\s\.\-\,/%]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ---------- Helpers: list-style answers ----------


def _split_tokens(s: str) -> List[str]:
    # Split on commas or semicolons into list-like tokens
    parts = re.split(r"[;,]", s)
    return [p.strip() for p in parts if p.strip()]


def _canon_token(t: str) -> str:
    # Canonicalize one token by removing extra punct and collapsing spaces
    t = _collapse_spaces_and_punct(t)
    t = re.sub(r"[^\w]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _list_tokens(s: str) -> Set[str]:
    return {_canon_token(p) for p in _split_tokens(s)}


def _list_tokens_match(a: str, b: str, mode: str = "equal") -> bool:
    """
    Compare comma/semicolon separated token lists after canonicalization.

    mode:
      - "equal"   : sets must be identical (strict)
      - "subset"  : gold ⊆ pred  (allow extra items in prediction)
      - "superset": pred ⊆ gold
      - "jaccard[:THRESH]" : Jaccard overlap >= THRESH (default 0.8)
    """
    sa, sb = _list_tokens(a), _list_tokens(b)
    if not sa or not sb:
        return False

    if mode == "equal":
        return sa == sb
    if mode == "subset":
        return sb.issubset(sa)  # gold ⊆ pred
    if mode == "superset":
        return sa.issubset(sb)
    if mode == "intersect":
        return len(sa & sb) > 0
    if mode.startswith("jaccard"):
        try:
            th = float(mode.split(":", 1)[1])
        except Exception:
            th = 0.8
        inter = len(sa & sb)
        uni = len(sa | sb)
        return (inter / uni) >= th
    # Fallback to strict if unknown
    return sa == sb


# ---------- Helpers: numeric handling ----------


def _extract_numbers_with_percent(s: str) -> List[Tuple[float, bool]]:
    """
    Return list of (value, has_percent) extracted in text order.
    E.g. "Positive correlation, 0.94" -> [(0.94, False)]
         "18.87%" -> [(18.87, True)]
    """
    out = []
    i = 0
    while True:
        m = _NUM_RE.search(s, i)
        if not m:
            break
        num_str = m.group()
        has_pct = False
        j = m.end()
        if j < len(s) and s[j] == "%":
            has_pct = True
            j += 1
        out.append((float(num_str), has_pct))
        i = j
    return out


def _rel_equal(a: float, b: float, tol: float) -> bool:
    if a == b:
        return True
    if b == 0:
        # near zero: tiny absolute tolerance
        return abs(a - b) <= max(1e-9, tol * 1e-6)
    return abs(a - b) / abs(b) <= tol


def _numbers_match(
    seq_a: List[Tuple[float, bool]], seq_b: List[Tuple[float, bool]], tol: float
) -> bool:
    """
    Compare ordered sequences of numbers, respecting percent flags.
    Accepts:
      - raw numeric equality within ±tol
      - percent/decimal scaling equivalence (e.g., 0.1887 vs 18.87%)
    """
    if not seq_a or not seq_b:
        return False
    if len(seq_a) != len(seq_b):
        return False

    for (va, pa), (vb, pb) in zip(seq_a, seq_b):
        if pa == pb:
            if not _rel_equal(va, vb, tol):
                return False
        else:
            # try raw compare
            if _rel_equal(va, vb, tol):
                continue
            # try scaling one side by 100
            if pa and _rel_equal(va, vb * 100.0, tol):
                continue
            if pb and _rel_equal(vb, va * 100.0, tol):
                continue
            return False
    return True


# ---------- Public API ----------


def answers_match(pred, gold, tol: float = 0.01, list_mode: str = "equal") -> bool:
    """
    Flexible comparator for QA evaluation.

    Handles:
      • case/accents/spacing/benign punctuation normalization
      • removes parentheticals
      • numeric comparison with ±tol relative tolerance (default 1%)
      • percent ↔ decimal equivalence (`0.1887` == `18.87%`)
      • embedded-number sequence comparison (order-preserving)
      • list answers with configurable policy: "equal", "subset", "superset", "jaccard[:THRESH]"

    Args:
      pred: system answer (any type; will be stringified)
      gold: gold answer   (any type; will be stringified)
      tol:  relative numeric tolerance (0.01 = 1%)
      list_mode: policy for list-like answers (comma/semicolon separated)

    Returns:
      bool: True if considered a match.
    """
    # 0) normalize text
    a = _collapse_spaces_and_punct(_basic_norm(pred))
    b = _collapse_spaces_and_punct(_basic_norm(gold))

    # 1) exact after normalization
    if a == b:
        return True

    # 2) numeric-only quick path (single scalar with optional %)
    try:
        fa = float(a.replace("%", ""))
        fb = float(b.replace("%", ""))
        has_pct_a = "%" in a
        has_pct_b = "%" in b

        if has_pct_a == has_pct_b:
            if _rel_equal(fa, fb, tol):
                return True
        else:
            # raw compare first
            if _rel_equal(fa, fb, tol):
                return True
            # scaling compare (percent vs decimal)
            if has_pct_a and _rel_equal(fa, fb * 100.0, tol):
                return True
            if has_pct_b and _rel_equal(fb, fa * 100.0, tol):
                return True
    except ValueError:
        pass

    # 3) numbers embedded in text: compare ordered sequences
    nums_a = _extract_numbers_with_percent(a)
    nums_b = _extract_numbers_with_percent(b)
    if nums_a and nums_b and _numbers_match(nums_a, nums_b, tol):
        return True

    # 4) list-like token comparison (commas/semicolons)
    if _list_tokens_match(a, b, mode=list_mode):
        return True

    # 5) degenerate duplicate-single-value list like "209,209" vs "209"
    ta, tb = _split_tokens(a), _split_tokens(b)
    if ta and tb and len(set(ta)) == 1 and len(tb) == 1:
        if re.sub(r"\s+", "", ta[0]) == re.sub(r"\s+", "", tb[0]):
            return True

    return False


class Answer(BaseModel):
    answers: list[float | int | str] | None = Field(
        None,
        description="Answer the input question by looking at the input table and intermediate steps \n"
        "You will generate a list of numbers and/or entities as answers, nothing else",
    )


async def qa_baseline(state):

    final_answer = AG(
        atype=Answer,
        instructions="Answer the input question by looking at the input table\n"
        "You will generate a list of numbers and/or entities as answers, nothing else",
    )

    final_answer = await (final_answer << state)
    if final_answer[0].answers:
        state.answer = ",".join([str(x) for x in final_answer[0].answers])
    return state


async def qa_with_intermediate_type(state):
    intermediate_ag = AG(
        instructions="Fill the information in the target type from information derived from the table."
    )
    intermediate_ag = await intermediate_ag.generate_atype(state.question)
    intermediate_answer = await (intermediate_ag << state)
    final_answer = AG(
        atype=Answer,
        instructions="Answer the input question by looking at the input table\n"
        "You will generate a list of numbers and/or entities as answers, nothing else",
    )

    final_answer = await (final_answer << str(intermediate_answer[0]) + str(state))
    state.answer = final_answer[0].answer
    return state


import os


async def run_benchmark(input_data, output_path):
    table_bench = AG.from_jsonl(input_data)
    table_bench.set_default_value("answer")
    table_bench.verbose_agent = True
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    for i in range(0, 9):
        tmp_bench = table_bench.clone()
        tmp_bench = tmp_bench.filter_states(start=i * 100, end=(i + 1) * 100)
        tmp_bench = await tmp_bench.amap(qa_baseline)
        tmp_bench.to_jsonl(os.path.join(output_path, f"output_{i}.json"))


def evaluate(system: str, gt: str, qtype=None, verbose=False):
    positives = 0
    negatives = 0

    for sys, gt in zip(system, gt):
        if qtype and sys.qtype == qtype or not qtype:

            if answers_match(sys.answer, gt.answer, list_mode="equal"):
                if verbose:
                    print("Good:", str(sys.answer) + "###" + str(gt.answer))
                positives += 1
            else:
                negatives += 1
                if verbose:
                    print("Bad:", str(sys.answer) + "###" + str(gt.answer))
    accuracy = positives / (positives + negatives)
    if verbose:
        print("accuracy: ", positives / (positives + negatives))
    return positives / (positives + negatives)


# system = AG.from_jsonl(str(OUTPUT_DIR / "/output_full.json"))
# gt = AG.from_jsonl(str(DATA_DIR / "TableBench.jsonl"))
# categories = set([x.qtype for x in system])
# for category in categories:
#     accuracy = evaluate(system, gt, qtype=category)
#     print(f"Evaluated Category {category} with accuracy {accuracy}")

asyncio.run(run_benchmark(str(DATA_DIR / "TableBench.jsonl"), str(OUTPUT_DIR)))

'''
    Embedding: intfloat/multilingual-e5-base
    Vector DB: FAISS 
    Hybrid: Semantic search + BM25 + RRF fusion 
    LLM: Google Gemini 3.1 flash lite 
'''

import os 
import re 
import json 
import numpy as np 
from sentence_transformers import SentenceTransformer
import faiss 
from dotenv import load_dotenv 
load_dotenv()
try: 
    import google.generativeai as genai 
    _GEMINI_OK = True 
except ImportError: 
    _GEMINI_OK = False 

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_INDEX_DIR = os.path.join(_ROOT, "rag", "index")
FAISS_PATH = os.path.join(_INDEX_DIR, "faiss.index")
META_PATH = os.path.join(_INDEX_DIR, "metadata.json")

EMBED_MODEL = "intfloat/multilingual-e5-base"
QUERY_PREFIX = "query: "

class _BM25:
    def __init__(self, k1: float = 1.5, b: float = 0.75):
        self.k1, self.b = k1, b
        self.idf: dict  = {}
        self.tf_docs: list = []
        self.doc_lens: list = []
        self.avg_len: float = 0.0
        self.n: int = 0
    @staticmethod
    def _tok(text: str) -> list[str]:
        return re.findall(r"\w+", text.lower())
    def fit(self, docs: list[str]):
        self.n = len(docs)
        tokenized = [self._tok(d) for d in docs]
        self.doc_lens = [len(t) for t in tokenized]
        self.avg_len  = sum(self.doc_lens) / self.n if self.n else 1.0
        # TF per doc
        self.tf_docs = []
        df: dict = {}
        for toks in tokenized:
            tf: dict = {}
            for w in toks:
                tf[w] = tf.get(w, 0) + 1
            self.tf_docs.append(tf)
            for w in set(toks):
                df[w] = df.get(w, 0) + 1
        # IDF
        for w, freq in df.items():
            self.idf[w] = np.log((self.n - freq + 0.5) / (freq + 0.5) + 1.0)
    def scores(self, query: str) -> np.ndarray:
        q_toks = self._tok(query)
        sc = np.zeros(self.n)
        for i, tf in enumerate(self.tf_docs):
            dl = self.doc_lens[i]
            for w in q_toks:
                if w not in tf:
                    continue
                f    = tf[w]
                idf  = self.idf.get(w, 0.0)
                num  = f * (self.k1 + 1)
                den  = f + self.k1 * (1 - self.b + self.b * dl / self.avg_len)
                sc[i] += idf * (num / den)
        return sc
# ══════════════════════════════════════════════════════════════════════════════
# GDIP RAG ENGINE
# ══════════════════════════════════════════════════════════════════════════════
class GDIPRagEngine:
    def __init__(self, gemini_api_key: str | None = None):
        self._embedder  : SentenceTransformer | None = None
        self._index     : faiss.Index | None = None
        self._meta      : list[dict] = []
        self._bm25      : _BM25 | None = None
        self._llm       = None
        self.use_gemini : bool = False
        self._load_index()
        self._load_embedder()
        self._fit_bm25()
        if gemini_api_key is None: 
            gemini_api_key = os.getenv("GEMINI_API_KEY")
        if gemini_api_key and _GEMINI_OK:
            self._init_gemini(gemini_api_key)
    # ── Setup ────────────────────────────────────────────────────────────────
    def _load_embedder(self):
        print(f"[RAG] Tải embedding model: {EMBED_MODEL} ...")
        self._embedder = SentenceTransformer(EMBED_MODEL)
    def _load_index(self):
        if not os.path.exists(FAISS_PATH):
            raise FileNotFoundError(
                f"Không tìm thấy FAISS index tại '{FAISS_PATH}'.\n"
                "Hãy chạy:  python rag/build_knowledge_base.py"
            )
        self._index = faiss.read_index(FAISS_PATH)
        with open(META_PATH, "r", encoding="utf-8") as f:
            self._meta = json.load(f)
        print(f"[RAG] FAISS index: {self._index.ntotal} vectors")
    def _fit_bm25(self):
        texts = [d["text"] for d in self._meta]
        self._bm25 = _BM25()
        self._bm25.fit(texts)
        print(f"[RAG] BM25 fitted: {len(texts)} docs")
    def _init_gemini(self, api_key: str):
        genai.configure(api_key=api_key)
        self._llm = genai.GenerativeModel(
            model_name="gemini-3.1-flash-lite",
            generation_config=genai.types.GenerationConfig(
                temperature=0.25,
                max_output_tokens=1024,
            ),
        )
        self.use_gemini = True
        print("[RAG] Gemini 3.1 Flash Lite sẵn sàng ✓")
    # ── Retrieve (Hybrid: Semantic + BM25 + RRF) ─────────────────────────────
    def _semantic_ranks(self, query: str) -> dict[int, int]:
        q_vec = self._embedder.encode(
            [QUERY_PREFIX + query],
            normalize_embeddings=True,
            convert_to_numpy=True,
        ).astype(np.float32)
        _, indices = self._index.search(q_vec, len(self._meta))
        return {int(idx): rank for rank, idx in enumerate(indices[0])}
    def _bm25_ranks(self, query: str) -> dict[int, int]:
        sc = self._bm25.scores(query)
        order = np.argsort(sc)[::-1]
        return {int(idx): rank for rank, idx in enumerate(order)}
    def retrieve(self, query: str, top_k: int = 6, rrf_k: int = 60) -> list[dict]:
        sem_r = self._semantic_ranks(query)
        bm_r  = self._bm25_ranks(query)
        n     = len(self._meta)
        # Reciprocal Rank Fusion
        rrf: dict[int, float] = {}
        for i in range(n):
            r_s = sem_r.get(i, n)
            r_b = bm_r.get(i, n)
            rrf[i] = 1.0 / (rrf_k + r_s) + 1.0 / (rrf_k + r_b)
        top_ids = sorted(rrf, key=rrf.__getitem__, reverse=True)[:top_k]
        results = []
        for idx in top_ids:
            doc = self._meta[idx].copy()
            doc["score"] = round(rrf[idx], 6)
            results.append(doc)
        return results
    # ── Generate ─────────────────────────────────────────────────────────────
    def _context_block(self, docs: list[dict]) -> str:
        return "\n".join(f"[{i+1}] {d['text']}" for i, d in enumerate(docs))
    def _gemini_answer(self, query: str, docs: list[dict]) -> str:
        context = self._context_block(docs)
        prompt = f"""Bạn là GDIP AI Analyst – chuyên gia phân tích kinh tế vĩ mô toàn cầu.
Nhiệm vụ: Trả lời câu hỏi của người dùng CHỈ dựa trên DỮ LIỆU GDIP được cung cấp bên dưới.
Quy tắc:
- Trả lời bằng Tiếng Việt, rõ ràng và súc tích.
- Ưu tiên dẫn số liệu cụ thể (xác suất rủi ro, chỉ số GDP, lạm phát...).
- Nếu hỏi nhiều quốc gia, trình bày dạng bảng hoặc bullet có cấu trúc.
- Nếu không đủ dữ liệu trong context, nói rõ thay vì bịa đặt.
- Kết thúc bằng 1 dòng nhận định ngắn (insight).
DỮ LIỆU GDIP:
{context}
CÂU HỎI: {query}
TRẢ LỜI:"""
        return self._llm.generate_content(prompt).text
    def _template_answer(self, query: str, docs: list[dict]) -> str:
        if not docs:
            return "Không tìm thấy thông tin liên quan trong cơ sở dữ liệu GDIP."
        risk_emoji = {"CAO": "🔴", "TRUNG BÌNH": "🟡", "THẤP": "🟢"}
        lines = ["Kết quả tìm kiếm từ GDIP Knowledge Base:\n"]
        for d in docs[:5]:
            src_label = {
                "risk_prediction":  "Dự báo rủi ro",
                "macro_forecast":   "Dự báo vĩ mô",
                "feature_snapshot": "Chỉ số kinh tế",
            }.get(d.get("source", ""), "Dữ liệu GDIP")
            emoji = ""
            if d.get("source") == "risk_prediction":
                lvl   = d.get("risk_level", "")
                emoji = risk_emoji.get(lvl, "") + " "
            lines.append(
                f"**{emoji}{d.get('country_name','')} ({d.get('year','')})** "
                f"– _{src_label}_\n> {d['text']}\n"
            )
        lines.append(
            "\n *Cấu hình Gemini API Key để nhận phân tích sâu hơn.*"
        )
        return "\n".join(lines)
    # ── Public API ────────────────────────────────────────────────────────────
    def ask(self, query: str, top_k: int = 6) -> dict:
        """
        Returns:
            {
              "answer":  str,
              "sources": list[{"country", "year", "source", "score", "snippet"}]
            }
        """
        docs   = self.retrieve(query, top_k=top_k)
        answer = (
            self._gemini_answer(query, docs)
            if self.use_gemini
            else self._template_answer(query, docs)
        )
        sources = [
            {
                "country": d.get("country_name", ""),
                "year":    d.get("year", ""),
                "source":  d.get("source", ""),
                "score":   d.get("score", 0.0),
                "snippet": d["text"][:130] + "…",
            }
            for d in docs[:4]
        ]
        return {"answer": answer, "sources": sources}
# ── Quick test ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    api_key = os.getenv("GEMINI_API_KEY")
    engine  = GDIPRagEngine(gemini_api_key=api_key)
    for q in [
        "Những quốc gia nào có rủi ro kinh tế cao nhất năm 2026?",
        "Dự báo lạm phát Việt Nam 2025 đến 2029?",
        "So sánh tăng trưởng GDP Việt Nam và Thái Lan",
    ]:
        print(f"\n{'─'*60}\n {q}\n")
        res = engine.ask(q)
        print(res["answer"])
        print("\n Sources:", [s["country"] for s in res["sources"]])
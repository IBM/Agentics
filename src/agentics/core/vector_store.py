from typing import List

import hnswlib
import numpy as np
from pydantic import BaseModel, ConfigDict
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
from sklearn.preprocessing import normalize


# ---- 1) Embedder (local) ----
class LocalEmbedder:
    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)  # 384-dim

    @property
    def dim(self) -> int:
        # all-MiniLM-L6-v2 → 384 dims
        return self.model.get_sentence_embedding_dimension()

    def embed(self, texts: List[str]) -> np.ndarray:
        vecs = self.model.encode(
            texts, normalize_embeddings=False, convert_to_numpy=True
        )
        return vecs.astype(np.float32)


from typing import Any, Dict, List

from pydantic import BaseModel, ConfigDict
from sklearn.cluster import KMeans
from sklearn.preprocessing import normalize


class HNSWStore:
    def __init__(
        self,
        dim: int,
        metric: str = "cosine",
        max_elements: int = 100_000,
        M: int = 16,
        ef_construction: int = 200,
        ef_search: int = 200,
    ):
        space = "cosine" if metric == "cosine" else "l2"
        self.metric = metric
        self.dim = dim
        self.index = hnswlib.Index(space=space, dim=dim)
        self.index.init_index(
            max_elements=max_elements, ef_construction=ef_construction, M=M
        )
        self.index.set_ef(ef_search)
        self.next_id = 0
        self.payloads: Dict[int, Any] = {}
        # keep raw vectors so we can cluster them
        self._vectors: List[np.ndarray] = []

    def add(self, vector: np.ndarray, payload: Any):
        v = np.asarray(vector, dtype=np.float32).reshape(1, -1)
        assert v.shape[1] == self.dim, f"Expected dim {self.dim}, got {v.shape[1]}"
        vid = self.next_id
        self.index.add_items(v, np.array([vid]))
        self.payloads[vid] = payload
        self._vectors.append(v.squeeze(0))
        self.next_id += 1
        return vid

    def search(self, query_vec: np.ndarray, k: int = 5):
        labels, dists = self.index.knn_query(
            np.asarray(query_vec, dtype=np.float32).reshape(1, -1), k=k
        )
        if dists.size == 0:
            return []
        # For cosine space, hnswlib distance = 1 - cosine_similarity
        if self.metric == "cosine":
            sims = 1.0 - dists[0]
            return [
                (float(sims[i]), self.payloads[int(labels[0][i])])
                for i in range(len(sims))
            ]
        # For L2, smaller is better; convert to a negative distance "score"
        scores = -dists[0]
        return [
            (float(scores[i]), self.payloads[int(labels[0][i])])
            for i in range(len(scores))
        ]


class VectorStore(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    # You must define LocalEmbedder elsewhere (or swap with your embedder)
    embedder: "LocalEmbedder" = None  # type: ignore
    store: HNSWStore | None = None

    def __init__(self, **data):
        super().__init__(**data)
        if self.embedder is None:
            self.embedder = LocalEmbedder()  # or OpenAIEmbedder()
        if self.store is None:
            self.store = HNSWStore(dim=self.embedder.dim, metric="cosine")

    def import_data(self, documents: List[str]):
        embs = self.embedder.embed(documents)
        for i, (t, v) in enumerate(zip(documents, embs)):
            self.store.add(v, {"text": t, "id": i})

    def search(self, query: str, k: int = 3):
        q_vec = self.embedder.embed([query])[0]
        return self.store.search(q_vec, k=k)

    def cluster(self, k: int = 5, normalize_vectors: bool = True):
        """
        K-Means clustering over all stored vectors.

        Returns:
          {
            "labels_by_id": {vid: label, ...},
            "centers": np.ndarray (k, dim),
            "items_by_cluster": {label: [payload, ...]}
          }
        """
        if self.store is None or not self.store._vectors:
            return {"labels_by_id": {}, "centers": None, "items_by_cluster": {}}

        X = np.vstack(self.store._vectors).astype(np.float32)
        if normalize_vectors and self.store.metric == "cosine":
            X = normalize(X)

        km = KMeans(n_clusters=k, random_state=42, n_init="auto")
        labels = km.fit_predict(X)

        labels_by_id = {vid: int(lbl) for vid, lbl in enumerate(labels)}
        items_by_cluster: Dict[int, List[Any]] = {}
        for vid, lbl in labels_by_id.items():
            items_by_cluster.setdefault(lbl, []).append(self.store.payloads[vid])
        return list(items_by_cluster.values())

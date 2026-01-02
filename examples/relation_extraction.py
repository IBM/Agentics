import asyncio
import time
from typing import Optional

from nltk.corpus import wordnet as wn
from pydantic import BaseModel

from agentics import AG
from agentics.core.default_types import Astr
from agentics.core.transducible_functions import With, transducible

N_TERMS = 500
N_CLUSTERS = 5
# llm = "watsonx/openai/gpt-oss-120b"


llm = AG.get_llm_provider()


class InputTerms(BaseModel):
    terms: list[str] = None


class Relation(BaseModel):
    subject: Optional[str] = None
    object: Optional[str] = None
    relation_type: Optional[str] = None


class Ontology(BaseModel):
    entities: Optional[list[str]] = None
    relations: Optional[list[Relation]] = None


extract_relations = Ontology << With(
    InputTerms,
    instructions="Derive an Ontology from the input"
    "objects by identifying relations among them",
    verbose_transduction=False,
    batch_size=20,
    llm=llm,
)


@transducible(llm=llm)
async def relation_extracton(input_objects: InputTerms) -> Ontology:
    ontology = await extract_relations(input_objects)
    return ontology


async def relation_extracton_map_reduce(
    input_objects: InputTerms, n_clusters: int = 10
) -> Ontology:
    terms = AG(states=[Astr(term) for term in input_objects.terms])

    clusters_ags = terms.cluster(n_partitions=n_clusters)
    clusters = [
        InputTerms(terms=[x.value for x in cluster.states]) for cluster in clusters_ags
    ]
    ontologies = await extract_relations(clusters)
    final_ontology = Ontology()
    final_ontology.entities = list(
        set([entity for ontology in ontologies for entity in (ontology.entities or [])])
    )
    final_ontology.relations = [
        Relation(
            subject=relation.subject,
            relation_type=relation.relation_type,
            object=relation.object,
        )
        for ontology in ontologies
        for relation in (ontology.relations or [])
    ]
    return final_ontology


async def evaluate_relation_extraction(outout_file: str = None, n_terms=10000):
    terms = random_frequent_terms(n=n_terms)
    # for i in range(1000, 1001, 500):

    print(f"--- Processing  {n_terms} instances ---")
    # start_time = time.time()
    # generate_terms = InputTerms << With(Astr, llm=llm, instructions=
    #     "Generate a list of financial terms")
    # financial_terms = await generate_terms(Astr(f"Generate a list of {i} terms related to finance."))

    # end_time = time.time()
    # print(f"Term Generation Time: {end_time - start_time:.2f} seconds")

    # used_terms = financial_terms
    used_terms = InputTerms(terms=terms)
    nclusters = n_terms // 50
    # start_time = time.time()
    print(f"Using {nclusters} clusters for map-reduce")

    ontology = await relation_extracton_map_reduce(used_terms, n_clusters=nclusters)
    # end_time = time.time()
    # print(f"Run {i+1} - Map-Reduce Relation Extraction Time: {end_time - start_time:.2f} seconds")
    print(
        f"Extracted {len(ontology.entities or [])} entities and {len(ontology.relations or [])} relations"
    )
    print(ontology.model_dump_json(indent=2))


import random


def random_frequent_terms(n=50, min_freq=100, pos=wn.NOUN):
    """
    Return up to `n` random lemma names from WordNet
    whose frequency (cntlist.rev) is >= min_freq.

    pos can be: wn.NOUN, wn.VERB, wn.ADJ, wn.ADV or None for all.
    """
    frequent_lemmas = []

    # Iterate over all lemma names (optionally filtered by POS)
    for name in wn.all_lemma_names(pos=pos):
        # Each lemma name can correspond to several Lemma objects (different synsets)
        for lemma in wn.lemmas(name, pos=pos):
            if lemma.count() >= min_freq:
                frequent_lemmas.append(lemma)

    if not frequent_lemmas:
        return []

    # Sample at most n lemmas
    sampled_lemmas = random.sample(frequent_lemmas, k=min(n, len(frequent_lemmas)))

    # If you just want the *words* (strings), dedupe names:
    terms = sorted({lemma.name() for lemma in sampled_lemmas})
    return terms


asyncio.run(evaluate_relation_extraction())

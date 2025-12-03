import asyncio
import time
from typing import Optional

from nltk.corpus import wordnet as wn
from pydantic import BaseModel

from agentics import AG
from agentics.core.default_types import Astr
from agentics.core.transducible_functions import With

N_TERMS = 100
N_CLUSTERS = 5


class InputData(BaseModel):
    data: list[str] = None


class Relation(BaseModel):
    subject: Optional[str] = None
    object: Optional[str] = None
    relation_type: Optional[str] = None


class Ontology(BaseModel):
    entities: Optional[list[str]] = None
    relations: Optional[list[Relation]] = None


extract_relations = Ontology << With(
    InputData,
    instructions="Derive an Ontology from the input"
    "objects by identifying relations among them",
    verbose_transduction=False,
    batch_size=20,
)


async def relation_extracton(input_objects: InputData) -> Ontology:
    ontology = await extract_relations(input_objects)
    return ontology


async def relation_extracton_map_reduce(
    input_objects: InputData, n_clusters: int = 10
) -> Ontology:
    terms = AG(states=[Astr(term) for term in input_objects.data])
    clusters_ags = terms.cluster(n_partitions=n_clusters)
    clusters = [
        InputData(data=[x.value for x in cluster.states]) for cluster in clusters_ags
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


async def evaluate_relation_extraction(outout_file: str = None):
    terms = get_top_100000_wordnet_terms()
    for i in range(100, 101, 1):

        print(f"--- Processing  {i} instances ---")
        start_time = time.time()
        financial_terms = await (
            InputData << Astr(f"Generate a list of {i} terms related to finance.")
        )
        end_time = time.time()
        print(f"Term Generation Time: {end_time - start_time:.2f} seconds")
        # start_time = time.time()
        # ontology = await (relation_extracton(financial_terms))
        # end_time = time.time()
        # print(f"Relation Extraction Time: {end_time - start_time:.2f} seconds")
        # print(f"Extracted {len(ontology.entities or [])} entities and {len(ontology.relations or [])} relations")

        # used_terms = InputData(data=terms[:i])
        used_terms = financial_terms
        start_time = time.time()
        ontology = await relation_extracton_map_reduce(
            used_terms, n_clusters=len(used_terms.data) % 50
        )
        end_time = time.time()
        print(
            f"Run {i+1} - Map-Reduce Relation Extraction Time: {end_time - start_time:.2f} seconds"
        )
        print(
            f"Extracted {len(ontology.entities or [])} entities and {len(ontology.relations or [])} relations"
        )
        print(ontology.model_dump_json(indent=2))


def get_top_100000_wordnet_terms():

    freq = {}

    for lemma in wn.all_lemma_names():
        freq[lemma] = freq.get(lemma, 0) + 1

    return sorted(freq, key=freq.get, reverse=True)[:100000]


asyncio.run(evaluate_relation_extraction())

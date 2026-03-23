#!/usr/bin/env python3
"""
Test script to verify infobox extraction works
"""
import asyncio
from typing import List, Optional

from pydantic import BaseModel, Field

from agentics import AG


class Relation(BaseModel):
    subject: Optional[str] = Field(default=None)
    relation: Optional[str] = Field(default=None)
    object: Optional[str] = Field(default=None)


class Relations(BaseModel):
    relations: Optional[List[Relation]] = Field(default=None)


class Infobox(BaseModel):
    entity: Optional[str] = Field(default=None)
    infobox: Optional[str] = Field(default=None, description="Infobox in JSON format")


async def extract_infobox(source: AG, entity: str) -> Infobox:
    """Extract infobox using Agentics transduction."""
    print(f"1. Searching for entity: {entity}")
    selected_sources = source.search(entity, k=10)
    print(f"   Search result type: {type(selected_sources)}")

    print("2. Creating relations AG")
    relations = AG(
        atype=Relations,
        instructions="Extract semantic relations from the source text",
        max_iter=1,
    )

    print("3. Transducing relations")
    relations = await (relations << selected_sources)
    print(f"   Relations result: {relations}")

    print("4. Creating infobox AG")
    infobox = AG(
        atype=Infobox,
        instructions=f"Extract infobox about {entity} from the source text",
        transduction_type="areduce",
    )

    print("5. Transducing infobox")
    infobox = await (infobox << relations)
    print(f"   Infobox result: {infobox}")
    print(f"   Infobox type: {type(infobox)}")
    print(
        f"   Infobox length: {len(infobox) if hasattr(infobox, '__len__') else 'N/A'}"
    )

    return infobox[0]


async def main():
    # Test with a simple CSV
    csv_path = "data/ibm_2025_report.csv"

    print(f"Loading corpus from: {csv_path}")
    with open(csv_path, "r") as f:
        corpus = AG.from_csv(f, max_rows=20)
    print(f"Corpus loaded: {type(corpus)}")

    entity = "IBM"
    print(f"\nExtracting infobox for: {entity}")

    try:
        infobox = await extract_infobox(corpus, entity)
        print(f"\n✓ Success!")
        print(f"Infobox: {infobox}")
        print(f"Infobox dict: {infobox.model_dump()}")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())

# Made with Bob

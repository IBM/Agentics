"""
Example demonstrating how to serialize and deserialize an Agentic instance.

This example shows:
1. Creating an Agentic with a custom Pydantic type
2. Serializing it to JSON
3. Deserializing it back from JSON
"""

import json
from typing import cast

from pydantic import BaseModel, Field

from agentics import AG


# Define a custom Pydantic type
class Person(BaseModel):
    """A simple person model"""

    name: str = Field(description="Person's name")
    age: int = Field(description="Person's age")
    occupation: str = Field(description="Person's occupation")


def main():
    # Create some sample data
    people = [
        Person(name="Alice", age=30, occupation="Engineer"),
        Person(name="Bob", age=25, occupation="Designer"),
        Person(name="Charlie", age=35, occupation="Manager"),
    ]

    # Create an Agentic instance
    ag = AG(atype=Person, states=people)  # type: ignore[call-arg]

    print("Original Agentic:")
    print(f"  Type: {ag.atype.__name__}")
    print(f"  Number of states: {len(ag.states)}")
    for person in ag.states:
        p = cast(Person, person)
        print(f"    - {p.name}, {p.age}, {p.occupation}")

    # Serialize to dictionary
    print("\n--- Serializing ---")
    serialized = ag.serialize()

    # Save to JSON file
    with open("agentic_serialized.json", "w") as f:
        json.dump(serialized, f, indent=2)
    print("Saved to 'agentic_serialized.json'")

    # Load from JSON file
    print("\n--- Deserializing ---")
    with open("agentic_serialized.json", "r") as f:
        loaded_data = json.load(f)

    # Deserialize back to Agentic
    # Option 1: Let it reconstruct the type from serialized data
    ag_restored = AG.deserialize(loaded_data)

    # Option 2: Provide the type explicitly (recommended if you have it)
    # ag_restored = AG.deserialize(loaded_data, atype=Person)

    print("Restored Agentic:")
    print(f"  Type: {ag_restored.atype.__name__}")
    print(f"  Number of states: {len(ag_restored.states)}")
    for person in ag_restored.states:
        p = cast(Person, person)
        print(f"    - {p.name}, {p.age}, {p.occupation}")

    # Verify the data matches
    print("\n--- Verification ---")
    assert len(ag.states) == len(ag_restored.states), "State count mismatch!"
    for i, (original, restored) in enumerate(zip(ag.states, ag_restored.states)):
        # Type cast to help the type checker understand these are Person objects
        orig_person = (
            original
            if isinstance(original, Person)
            else Person(**original.model_dump())
        )
        rest_person = (
            restored
            if isinstance(restored, Person)
            else Person(**restored.model_dump())
        )

        assert (
            orig_person.name == rest_person.name
        ), f"Name mismatch: {orig_person.name} != {rest_person.name}"
        assert (
            orig_person.age == rest_person.age
        ), f"Age mismatch: {orig_person.age} != {rest_person.age}"
        assert orig_person.occupation == rest_person.occupation, f"Occupation mismatch"

    print("✓ All data successfully serialized and deserialized!")

    # Show what's in the serialized data
    print("\n--- Serialized Data Structure ---")
    print(f"Keys in serialized data: {list(serialized.keys())}")
    print(f"Number of states: {len(serialized['states'])}")
    print(f"Atype name: {serialized['atype_name']}")
    print(f"Has atype code: {serialized['atype_code'] is not None}")
    print(f"Has atype schema: {serialized['atype_schema'] is not None}")


if __name__ == "__main__":
    main()

# Made with Bob

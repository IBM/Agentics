"""
Test Script for Schema Manager Service

This script demonstrates how to use the Schema Manager API programmatically.
"""

import json
import time

import requests

API_URL = "http://localhost:5003/api/schemas"
BASE_URL = "http://localhost:5003"


def test_health():
    """Test health check endpoint"""
    print("\n" + "=" * 60)
    print("Testing Health Check")
    print("=" * 60)

    response = requests.get(f"{BASE_URL}/health")
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        print(f"Response: {response.json()}")
    else:
        print(f"Error: {response.text[:200]}")


def test_create_type_with_form():
    """Test creating a type using form fields"""
    print("\n" + "=" * 60)
    print("Creating Type with Form Builder")
    print("=" * 60)

    type_data = {
        "name": "TestUser",
        "fields": [
            {
                "name": "user_id",
                "type": "int",
                "description": "Unique user identifier",
                "required": True,
                "default": None,
            },
            {
                "name": "username",
                "type": "str",
                "description": "User's display name",
                "required": True,
                "default": None,
            },
            {
                "name": "email",
                "type": "str",
                "description": "User's email address",
                "required": False,
                "default": None,
            },
            {
                "name": "tags",
                "type": "List[str]",
                "description": "User tags",
                "required": False,
                "default": None,
            },
        ],
    }

    response = requests.post(f"{API_URL}/types", json=type_data)
    print(f"Status: {response.status_code}")
    result = response.json()

    if result.get("success"):
        print(f"✓ Type '{result['name']}' created successfully!")
        print(f"  Schema ID: {result['schema_id']}")
        print(f"  Fields: {len(result['fields'])}")
    else:
        print(f"✗ Error: {result.get('error')}")

    return result.get("success", False)


def test_create_type_with_code():
    """Test creating a type using Python code"""
    print("\n" + "=" * 60)
    print("Creating Type with Code Editor")
    print("=" * 60)

    code = """from pydantic import BaseModel, Field
from typing import Optional, List


class Product(BaseModel):
    product_id: int = Field(description="Unique product identifier")
    name: str = Field(description="Product name")
    price: float = Field(description="Product price")
    in_stock: bool = Field(default=True, description="Availability status")
    categories: Optional[List[str]] = Field(default=None, description="Product categories")
"""

    type_data = {"name": "Product", "code": code}

    response = requests.post(f"{API_URL}/types", json=type_data)
    print(f"Status: {response.status_code}")
    result = response.json()

    if result.get("success"):
        print(f"✓ Type '{result['name']}' created successfully!")
        print(f"  Schema ID: {result['schema_id']}")
        print(f"  Fields: {len(result['fields'])}")
    else:
        print(f"✗ Error: {result.get('error')}")

    return result.get("success", False)


def test_list_types():
    """Test listing all types"""
    print("\n" + "=" * 60)
    print("Listing All Types")
    print("=" * 60)

    response = requests.get(f"{API_URL}/types")
    result = response.json()

    if "types" in result:
        print(f"Found {len(result['types'])} type(s):")
        for type_info in result["types"]:
            print(
                f"  - {type_info['name']} (v{type_info['version']}, ID: {type_info['id']})"
            )
    else:
        print("No types found or error occurred")


def test_get_type(type_name: str):
    """Test getting a specific type"""
    print("\n" + "=" * 60)
    print(f"Getting Type: {type_name}")
    print("=" * 60)

    response = requests.get(f"{API_URL}/types/{type_name}")

    if response.status_code == 200:
        result = response.json()
        print(f"✓ Type '{result['name']}' retrieved successfully!")
        print(f"\nFields:")
        for field in result["fields"]:
            req = "required" if field["required"] else "optional"
            print(f"  - {field['name']}: {field['type']} ({req})")
            if field["description"]:
                print(f"    Description: {field['description']}")

        print(f"\nPython Code:")
        print("-" * 60)
        print(result["code"])
        print("-" * 60)
    else:
        print(f"✗ Error: {response.json().get('error')}")


def test_update_type(type_name: str):
    """Test updating a type"""
    print("\n" + "=" * 60)
    print(f"Updating Type: {type_name}")
    print("=" * 60)

    # Add a new field
    type_data = {
        "fields": [
            {
                "name": "user_id",
                "type": "int",
                "description": "Unique user identifier",
                "required": True,
                "default": None,
            },
            {
                "name": "username",
                "type": "str",
                "description": "User's display name",
                "required": True,
                "default": None,
            },
            {
                "name": "email",
                "type": "str",
                "description": "User's email address",
                "required": False,
                "default": None,
            },
            {
                "name": "tags",
                "type": "List[str]",
                "description": "User tags",
                "required": False,
                "default": None,
            },
            {
                "name": "created_at",
                "type": "str",
                "description": "Account creation timestamp",
                "required": False,
                "default": None,
            },
        ]
    }

    response = requests.put(f"{API_URL}/types/{type_name}", json=type_data)
    result = response.json()

    if result.get("success"):
        print(f"✓ Type '{result['name']}' updated successfully!")
        print(f"  New Schema ID: {result['schema_id']}")
        print(f"  Fields: {len(result['fields'])}")
    else:
        print(f"✗ Error: {result.get('error')}")


def test_validate_code():
    """Test code validation"""
    print("\n" + "=" * 60)
    print("Validating Python Code")
    print("=" * 60)

    # Valid code
    valid_code = """from pydantic import BaseModel

class ValidType(BaseModel):
    field1: str
    field2: int
"""

    response = requests.post(f"{API_URL}/validate", json={"code": valid_code})
    result = response.json()

    if result.get("valid"):
        print(f"✓ Code is valid! Model name: {result['name']}")
    else:
        print(f"✗ Code is invalid: {result.get('error')}")

    # Invalid code
    print("\nTesting invalid code...")
    invalid_code = "this is not valid python"

    response = requests.post(f"{API_URL}/validate", json={"code": invalid_code})
    result = response.json()

    if not result.get("valid"):
        print(f"✓ Correctly identified invalid code")
        print(f"  Error: {result.get('error')}")


def test_delete_type(type_name: str):
    """Test deleting a type"""
    print("\n" + "=" * 60)
    print(f"Deleting Type: {type_name}")
    print("=" * 60)

    response = requests.delete(f"{API_URL}/types/{type_name}")
    result = response.json()

    if result.get("success"):
        print(f"✓ Type '{type_name}' deleted successfully!")
    else:
        print(f"✗ Error: {result.get('error')}")


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("🧪 Schema Manager API Test Suite")
    print("=" * 60)
    print("\nPrerequisites:")
    print("  1. Schema Manager service running on http://localhost:5001")
    print("  2. Kafka and Schema Registry running")
    print("\nStarting tests in 2 seconds...")
    time.sleep(2)

    try:
        # Test health
        test_health()

        # Test creating types
        test_create_type_with_form()
        time.sleep(1)

        test_create_type_with_code()
        time.sleep(1)

        # Test listing types
        test_list_types()
        time.sleep(1)

        # Test getting specific types
        test_get_type("TestUser")
        time.sleep(1)

        test_get_type("Product")
        time.sleep(1)

        # Test updating a type
        test_update_type("TestUser")
        time.sleep(1)

        # Test code validation
        test_validate_code()
        time.sleep(1)

        # Test listing after updates
        test_list_types()
        time.sleep(1)

        # Test deleting types
        test_delete_type("TestUser")
        time.sleep(1)

        test_delete_type("Product")
        time.sleep(1)

        # Final list
        test_list_types()

        print("\n" + "=" * 60)
        print("✅ All tests completed!")
        print("=" * 60)

    except requests.exceptions.ConnectionError:
        print("\n❌ Error: Could not connect to Schema Manager service")
        print("   Make sure the service is running: python schema_manager_service.py")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()

# Made with Bob

import json
import argparse
from pathlib import Path
from typing import Optional, List, Dict, Any

def sort_jsonl_file(input_file: Path, output_file: Path) -> int:
    """
    Read a JSONL file, sort items by metadata_id and qid, and save to a new file.
    
    Returns number of items sorted
    """
    items = []
    
    # Load all items from the file
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as infile:
        for line in infile:
            if line.strip():
                try:
                    item = json.loads(line)
                    items.append(item)
                except json.JSONDecodeError as e:
                    print(f"  Warning: Skipped malformed JSON line in {input_file.name}: {e}")
    
    if not items:
        print(f"  Warning: No items found in {input_file.name}")
        return 0
    
    # Sort by metadata_id first, then by qid (both ascending)
    # Convert to int for proper numerical sorting
    def sort_key(item):
        metadata_id = item.get('metadata_id', 0)
        qid = item.get('qid', 0)
        
        # Convert to int if they're not already
        if isinstance(metadata_id, str):
            try:
                metadata_id = int(metadata_id)
            except (ValueError, TypeError):
                metadata_id = 0
        
        if isinstance(qid, str):
            try:
                qid = int(qid)
            except (ValueError, TypeError):
                qid = 0
        
        return (metadata_id, qid)
    
    items.sort(key=sort_key)
    
    # Write sorted items to output file
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for item in items:
            outfile.write(json.dumps(item) + '\n')
    
    return len(items)

def process_folder(folder_path: str) -> None:
    """
    Walk through a folder, find all .jsonl files, and sort them.
    
    Args:
        folder_path: Path to the folder to process
    """
    folder = Path(folder_path)
    
    if not folder.exists():
        print(f"Error: Folder does not exist: {folder}")
        return
    
    if not folder.is_dir():
        print(f"Error: Not a directory: {folder}")
        return
    
    # Find all .jsonl files
    jsonl_files = list(folder.rglob('*.jsonl'))
    
    # Always filter out already processed files
    jsonl_files = [f for f in jsonl_files if not f.name.endswith('_sorted_tmp.jsonl')]
    
    if not jsonl_files:
        print(f"No .jsonl files found in {folder}")
        return
    
    print(f"Found {len(jsonl_files)} .jsonl files in {folder}\n")
    
    total_items = 0
    successful_files = 0
    failed_files = 0
    
    for jsonl_file in sorted(jsonl_files):
        print(f"Sorting: {jsonl_file.name}")
        
        try:
            # Create temporary output file
            output_file = jsonl_file.parent / f"{jsonl_file.stem}_sorted_tmp.jsonl"
            
            num_items = sort_jsonl_file(jsonl_file, output_file)
            total_items += num_items
            successful_files += 1
            print(f"  ✓ Sorted {num_items} items")
            
            # Replace original file with sorted file
            output_file.replace(jsonl_file)

            # Remove original file
            jsonl_file.unlink()
            output_file.rename(jsonl_file)
            print(f"  ✓ Replaced: {jsonl_file.name}")
            
        except Exception as e:
            print(f"  ✗ Error processing {jsonl_file.name}: {e}\n")
            failed_files += 1
    
    # Print summary
    print("=" * 80)
    print("SORTING SUMMARY")
    print("=" * 80)
    print(f"Folder: {folder}")
    print(f"Files processed: {successful_files}")
    print(f"Files failed: {failed_files}")
    print(f"Total items sorted: {total_items}")
    print("=" * 80)

def main():
    parser = argparse.ArgumentParser(
        description="Sort JSONL files by metadata_id and qid"
    )
    parser.add_argument(
        "--folder",
        type=str,
        help="Path to the folder containing .jsonl files"
    )
    
    args = parser.parse_args()
    process_folder(args.folder)

if __name__ == "__main__":
    main()

import json
import argparse
from pathlib import Path
from typing import Optional

def reduce_jsonl_file(input_file: Path, output_file: Path) -> tuple:
    """
    Read a JSONL file, drop metadata and dbs fields, and save to a new file.
    
    Returns (num_items, num_dropped_fields)
    """
    num_items = 0
    num_dropped_fields = 0
    
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        for line in infile:
            if line.strip():
                try:
                    item = json.loads(line)
                    
                    # Drop metadata and dbs fields if they exist
                    if 'metadata' in item:
                        del item['metadata']
                        num_dropped_fields += 1
                    if 'dbs' in item:
                        del item['dbs']
                        num_dropped_fields += 1
                    
                    # Write the reduced item
                    outfile.write(json.dumps(item) + '\n')
                    num_items += 1
                    
                except json.JSONDecodeError as e:
                    print(f"  Warning: Skipped malformed JSON line in {input_file.name}: {e}")
                except Exception as e:
                    print(f"  Error processing line in {input_file.name}: {e}")
    
    return num_items, num_dropped_fields

def process_folder(folder_path: str) -> None:
    """
    Walk through a folder, find all .jsonl files, and reduce them.
    
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
    
    if not jsonl_files:
        print(f"No .jsonl files found in {folder}")
        return
    
    print(f"Found {len(jsonl_files)} .jsonl files in {folder}\n")
    
    total_items = 0
    total_dropped_fields = 0
    successful_files = 0
    failed_files = 0
    
    for jsonl_file in sorted(jsonl_files):
        # Skip files that end with _short_tmp.jsonl (already processed)
        if jsonl_file.name.endswith('_short_tmp.jsonl'):
            print(f"Skipping (already processed): {jsonl_file.name}")
            continue
        
        # Create output filename
        output_file = jsonl_file.parent / f"{jsonl_file.stem}_short_tmp.jsonl"
        
        try:
            print(f"Processing: {jsonl_file.name}")
            num_items, num_dropped_fields = reduce_jsonl_file(jsonl_file, output_file)
            
            total_items += num_items
            total_dropped_fields += num_dropped_fields
            successful_files += 1
            
            # Get file sizes
            input_size = jsonl_file.stat().st_size / (1024 * 1024)  # MB
            output_size = output_file.stat().st_size / (1024 * 1024)  # MB
            reduction = ((input_size - output_size) / input_size) * 100 if input_size > 0 else 0
            
            print(f"  ✓ Items: {num_items}, Dropped fields: {num_dropped_fields}")
            print(f"    Size: {input_size:.2f}MB → {output_size:.2f}MB (reduced by {reduction:.1f}%)")
            print(f"    Saved to: {output_file.name}\n")
            
        except Exception as e:
            print(f"  ✗ Error processing {jsonl_file.name}: {e}\n")
            failed_files += 1
    
    # Cleanup: remove original files and rename temp files
    print("Cleaning up: replacing original files with reduced versions...\n")
    for jsonl_file in sorted(jsonl_files):
        if jsonl_file.name.endswith('_short_tmp.jsonl'):
            continue
        
        output_file = jsonl_file.parent / f"{jsonl_file.stem}_short_tmp.jsonl"
        
        if output_file.exists():
            try:
                # Remove original file
                jsonl_file.unlink()
                # Rename temp file to original name
                output_file.rename(jsonl_file)
                print(f"  ✓ Replaced: {jsonl_file.name}")
            except Exception as e:
                print(f"  ✗ Error during cleanup for {jsonl_file.name}: {e}")
    
    print()
    
    # Print summary
    print("=" * 80)
    print("PROCESSING SUMMARY")
    print("=" * 80)
    print(f"Folder: {folder}")
    print(f"Files processed: {successful_files}")
    print(f"Files failed: {failed_files}")
    print(f"Total items processed: {total_items}")
    print(f"Total fields dropped: {total_dropped_fields}")
    print("=" * 80)

def main():
    parser = argparse.ArgumentParser(
        description="Reduce JSONL files by dropping metadata and dbs fields"
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

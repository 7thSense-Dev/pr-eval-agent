#!/usr/bin/env python3
import zipfile
import os
from pathlib import Path

zip_path = r'C:\Users\USER\pr-eval-agent\tmp\poc\1627_1777420316752_recrui8\uploaded_to_eval_agent\files.zip'
extract_dir = r'C:\Users\USER\pr-eval-agent\tmp\poc\1627_1777420316752_recrui8\uploaded_to_eval_agent\files_extracted'

if os.path.exists(extract_dir):
    import shutil
    shutil.rmtree(extract_dir)

with zipfile.ZipFile(zip_path, 'r') as z:
    z.extractall(extract_dir)

# List all JSON files
print("JSON files in files.zip:")
json_count = 0
for root, dirs, files in os.walk(extract_dir):
    for f in files:
        if f.endswith('.json'):
            full_path = os.path.join(root, f)
            print(f"  {full_path}")
            json_count += 1

print(f"\nTotal JSON files: {json_count}")

# Also check directory structure
print("\nDirectory structure:")
for root, dirs, files in os.walk(extract_dir):
    level = root.replace(extract_dir, '').count(os.sep)
    indent = ' ' * 2 * level
    print(f'{indent}{os.path.basename(root)}/')
    subindent = ' ' * 2 * (level + 1)
    for file in files:
        print(f'{subindent}{file}')

import os
import re

versions_dir = r'g:\Openweb\backend\open_webui\migrations\versions'
migrations = {}

for filename in os.listdir(versions_dir):
    if filename.endswith('.py') and filename != '__init__.py':
        filepath = os.path.join(versions_dir, filename)
        with open(filepath, 'r') as f:
            content = f.read()
            rev_match = re.search(r'revision\s*[:\w\s]*=\s*["\']([^"\']+)["\']', content)
            down_match = re.search(r'down_revision\s*[:\w\s,]*=\s*(?:["\']([^"\']+)["\']|None)', content)
            
            if rev_match:
                rev = rev_match.group(1)
                down = down_match.group(1) if down_match and down_match.group(1) else None
                migrations[rev] = {
                    'filename': filename,
                    'down': down
                }

print(f"Found {len(migrations)} migrations.")
print("Migration Graph:")
for rev, data in sorted(migrations.items(), key=lambda x: x[0]):
    print(f"{rev} ({data['filename']}) -> {data['down']}")

# Find heads
heads = set(migrations.keys()) - set(m['down'] for m in migrations.values() if m['down'])
print(f"\nHeads: {heads}")

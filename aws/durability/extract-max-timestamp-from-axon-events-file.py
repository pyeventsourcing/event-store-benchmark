import os

file_path = "./axon-recovered-events/00000000000000000000.events"

# print("Extracting timestamps from recovered Axon Server events file:", file_path)

if not os.path.exists(file_path):
    print(f"❌ File not found at {file_path}")
    exit(1)

with open(file_path, "rb") as f:
    data = f.read()

timestamps = []
pos = 0

while True:
    # Find the next occurrence of "timestamp"
    idx = data.find(b"timestamp", pos)
    if idx == -1:
        break

    # Move past "timestamp" (9 bytes)
    p = idx + 9

    # Locate the Protobuf string field tag (\x0a) in the next 10 bytes
    tag_idx = data.find(b"\x0a", p, p + 10)
    if tag_idx != -1:
        # Read the exact length byte specified by Protobuf
        length = data[tag_idx + 1]

        # Read exactly 'length' bytes
        if 1 <= length <= 30:
            str_bytes = data[tag_idx + 2 : tag_idx + 2 + length]
            if str_bytes.isdigit():
                timestamps.append(int(str_bytes.decode("ascii")))

    pos = idx + 9

print(f"Extracted timestamp count: {len(timestamps)}")
if timestamps:
    min_ts = min(timestamps)
    max_ts = max(timestamps)
    print(f"Max extracted timestamp: {max_ts} ns")
else:
    print("❌ No timestamp patterns found in file.")
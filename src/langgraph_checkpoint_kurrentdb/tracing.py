# Format a single tree line
def format_line(indent, timestamp, source, step, label, latency, is_last):
    prefix = "│  " * indent
    branch = "└─" if is_last else "├─"
    ts_str = timestamp.strftime("%H:%M:%S.%f")[:-3]
    source_prefix = f"{source} :: " if source != "normal_run" else ""
    return f"{prefix}{branch} [{ts_str}] {source_prefix}Step {step}: [{label}] ({latency} ms)"

# Print the tree
# assumes only 1 level subgraph at the moment
def print_tree(thread_id, data):
    # Build and sort entries
    entries = []
    for source, step, timestamp, label, latency in data:
        indent = 1 if source != thread_id else 0
        entries.append({
            "source": source,
            "timestamp": timestamp,
            "step": step,
            "label": label,
            "latency": latency,
            "indent": indent
        })

    entries.sort(key=lambda x: x["timestamp"])

    print("├─", thread_id)
    for i, entry in enumerate(entries):
        is_last = i == len(entries) - 1 or entries[i + 1]["indent"] < entry["indent"]
        line = format_line(
            indent=entry["indent"],
            timestamp=entry["timestamp"],
            source=entry["source"],
            step=entry["step"],
            label=entry["label"],
            latency=entry["latency"],
            is_last=is_last
        )
        print(line)

# Format a single tree line
import datetime
from xml.etree.ElementTree import indent
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import Resource


def format_line(indent, timestamp, source, step, label, latency, is_last):
    prefix = "│  " * indent
    branch = "└─" if is_last else "├─"
    ts_str = timestamp.strftime("%H:%M:%S.%f")[:-3]
    source_prefix = f"{source} :: " if source != "normal_run" else ""
    return f"{prefix}{branch} [{ts_str}] {source_prefix}Step {step}: [{label}] ({latency} ms)"

def export_tree_otel(thread_id, data, span_processor, trace):
    trace.set_tracer_provider(TracerProvider(resource=Resource(attributes={
        "service.name": thread_id
    })))
    trace.get_tracer_provider().add_span_processor(span_processor)
    graphs = {}
    for source, step, timestamp, label, latency, ns in data.__reversed__():
        ns = "root|" + ns
        indent = len(ns.split("|"))
        node = "root"
        if indent > 1:
            nodes = ns.split("|")
            node = nodes[-1]
            if node == "":
                node = "root"
        if node != "root":
            node = node.split(":")[0]
        if node not in graphs:
            graphs[node] = []
        graphs[node].append({
            "source": source,
            "timestamp": timestamp,
            "step": step,
            "label": label,
            "latency": latency,
            "indent": indent,
            "node": node
        })

    # Configure tracer
    trace.set_tracer_provider(TracerProvider(resource=Resource(attributes={
        "service.name": thread_id
    })))
    tracer = trace.get_tracer(__name__)

    #DFS
    traversal = []  #treat as stack and init
    root_span = tracer.start_span(thread_id)
    root_context = trace.set_span_in_context(root_span)

    for child_node in graphs["root"]:
        traversal.append((child_node, 0, root_context))

    traversed = set()
    while len(traversal) > 0:
        head, level, ctx = traversal.pop()
        # Start a child span using parent context
        start_time_unix_nano = int(head["timestamp"].timestamp() * 1_000_000_000)
        duration_nano = head["latency"] * 1_000_000
        end_time = start_time_unix_nano + duration_nano

        #enrich child span
        child_span = tracer.start_span(head["source"] + ":" + head["label"], context=ctx, start_time=start_time_unix_nano)
        child_span.set_attribute("source", head["source"])
        child_span.set_attribute("label", head["label"])
        child_span.set_attribute("step", head["step"])
        child_span.set_attribute("latency", head["latency"])
        child_span.set_attribute("thread_id", thread_id)
        child_span.end(end_time=end_time)

        if head["label"] in graphs: #a subgraph was found
            parent_ctx = trace.set_span_in_context(child_span)
            for child_node in graphs[head["label"]]: #nodes under this head
                if head["label"] + child_node["label"] not in traversed:
                    traversal.append((child_node, level + 1, parent_ctx))


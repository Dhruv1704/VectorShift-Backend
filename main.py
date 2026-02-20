from fastapi import FastAPI
from pydantic import BaseModel
from collections import deque
from typing import Any, Dict, List, TypedDict
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()


class Edge(TypedDict):
    id: str
    source: str
    target: str


class ParseRequest(BaseModel):
    nodes: List[Any]
    edges: List[Edge]



app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/')
def read_root():
    return {'Ping': 'Pong'}

@app.post('/pipelines/parse')
def parse_pipeline(payload: ParseRequest) -> Dict[str, int | bool]:
    nodes = payload.nodes
    edges = payload.edges
    return {
        "num_nodes": len(nodes),
        "num_edges": len(edges),
        "is_dag": is_dag(edges),
    }


def is_dag(edges: List[Edge]) -> bool:
    graph: Dict[str, List[str]] = {}
    indegree: Dict[str, int] = {}

    for edge in edges:
        u = edge["source"]
        v = edge["target"]
        graph.setdefault(u, []).append(v)
        graph.setdefault(v, [])
        indegree.setdefault(u, 0)
        indegree[v] = indegree.get(v, 0) + 1

    q = deque([node for node in indegree if indegree[node] == 0])
    seen = 0

    while q:
        node = q.popleft()
        seen += 1
        for nxt in graph[node]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                q.append(nxt)

    return seen == len(indegree)

#!/usr/bin/env python3
"""
Very simple HTTP server in python for logging requests
Usage::
    ./server.py [<port>]
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import logging
import json
import threading
import requests
import argparse
import time

logging.basicConfig(level=logging.INFO)

node_id = ("","")
N = NN = P = L = ("","")
cluster_nodes = []
nick = ""
voting = False


def log_message(msg, sender, color=""):
    logging.info(f"[{sender}]: {msg}\n")


def get_current_state():
    return {
        "Node": node_id,
        "N": N,
        "NN": NN,
        "P": P,
        "L": L,
        "cluster_nodes": cluster_nodes
    }

# class ClusterNode:
#
#     def __init__(self, node_id, nickname):
#         self.node_id = node_id
#         self.nickname = nickname
#         self.N = self.NN = self.P = self.L = node_id
#         self.cluster_nodes = []


def join_node(joined_node_id):
    global P
    requests.post(f"http://{joined_node_id[0]}:{joined_node_id[1]}", json={
        "msg_type": "join",
        "from": node_id,
    })
    P = joined_node_id


def handle_join(params, from_):
    global N, NN, P
    # Check if clusters consists of only one node. Depending on this value,
    # algorithm will change.
    single_node_cluster = N == P == NN

    # Step 1 - Replace N and NN pointers.
    prev_N = N
    prev_NN = NN
    NN = N
    N = from_
    # Step 2 - Reply with N, NN, L pointers for newly joined node.
    requests.post(f"http://{from_[0]}:{from_[1]}", json={
        "msg_type": "join_reply",
        "from": node_id,
        "params": {
            "N": prev_N,
            "NN": from_ if single_node_cluster else prev_NN,
            "L": L,
        }
    })
    # Step 3 - Change NN of previous node to newly_joined.
    if not single_node_cluster:
        requests.post(f"http://{P[0]}:{P[1]}", json={
            "msg_type": "change_nn",
            "from": node_id,
            "params": {
                "NN": from_
            }
        })
    # Step 4 - Change P.
    if single_node_cluster:
        P = from_
    else:
        requests.post(f"http://{prev_N[0]}:{prev_N[1]}", json={
            "msg_type": "change_p",
            "from": node_id,
            "params": {
                "P": from_
            }
        })
    # Step 5 - The leader must know about all nodes in the cluster, so
    # every newly joined node has to register itself.
    if node_id == L:
        cluster_nodes.append(from_)
    else:
        requests.post(f"http://{L[0]}:{L[1]}", json={
            "msg_type": "register_node",
            "from": node_id,
            "params": {
                "new_node": from_
            }
        })


def handle_register_node(params, from_):
    cluster_nodes.append(params["new_node"])


def handle_deregister_node(params, from_):
    # print("Removed node:", params["removed_node"])
    cluster_nodes.remove(params["removed_node"])


def handle_change_p(params, from_):
    global P
    P = params["P"]


def handle_change_nn(params, from_):
    global NN
    NN = params["NN"]


def handle_join_reply(params, from_):
    global N, NN, L
    N = params["N"]
    NN = params["NN"]
    L = params["L"]


def handle_log_chat_msg(params, from_):
    # TODO(valyavka): Add color to the message
    log_message(params['chat_msg'], params['sender'])


def handle_send_chat_msg(params, from_):
    global voting
    sender = params.get("sender", f"{node_id}, {nick}")

    if node_id == L:
        log_message(params['chat_msg'], sender)
        unreachable_node = None
        for node in cluster_nodes:
            try:
                requests.post(f"http://{node[0]}:{node[1]}", json={
                    "msg_type": "log_chat_msg",
                    "from": node_id,
                    "params": {
                        "sender": sender,
                        "chat_msg": params["chat_msg"]
                    }
                })
            except requests.ConnectionError:
                unreachable_node = node

        # If there are some nodes that are down, repair the topology
        if unreachable_node is not None:
            if unreachable_node == N:
                remove_n_and_repair_topology(params, from_)
            else:
                requests.post(f"http://{N[0]}:{N[1]}", json={
                    "msg_type": "dead_node_detected",
                    "from": node_id,
                    "params": {
                        "dead_node": unreachable_node
                    }
                })
    elif node_id != (L[0],L[1]):
        try:
            requests.post(f"http://{L[0]}:{L[1]}", json={
                "msg_type": "send_chat_msg",
                "from": node_id,
                "params": {
                    "chat_msg": params["chat_msg"],
                    "sender": sender
                }
            })
        except requests.ConnectionError:
            # If cannot connect to the leader than start leader election
            print("Starting a new election with {node_id}")
            try:
                requests.post(f"http://{N[0]}:{N[1]}", json={
                    "msg_type": "election",
                    "from": node_id,
                    "params": {
                        "max_id": node_id,
                        "msg_to_retry": params["chat_msg"],
                        "sender": sender,
                    }
                })
            except requests.ConnectionError:
                print(f"For repairing"+str(get_current_state()))
                print(f"Start Repairing at {node_id[1]} with params {params} and my N is {N}")
                remove_n_and_repair_topology(params, from_)
                voting = True
                requests.post(f"http://{N[0]}:{N[1]}", json={
                    "msg_type": "election",
                    "from": node_id,
                    "params": {
                        "max_id": node_id,
                        "msg_to_retry": params["chat_msg"],
                        "sender": sender,
                    }
                })
            # handle_election({"max_id": node_id,
            #                  "msg_to_retry": params["chat_msg"],
            #                  "sender": sender}, None)


def handle_dead_node_detected(params, from_):
    dead_node = params["dead_node"]
    if N == dead_node:
        # print(f"Repairing on {node_id}")
        remove_n_and_repair_topology(params, from_)
    else:
        requests.post(f"http://{N[0]}:{N[1]}", json={
            "msg_type": "dead_node_detected",
            "from": node_id,
            "params": {
                "dead_node": dead_node
            }
        })


def remove_n_and_repair_topology(params, from_):
    global N, NN, P, L, cluster_nodes
    # Step 0 - Deregister N node from a leader.
    try:
        requests.post(f"http://{L[0]}:{L[1]}", json={
            "msg_type": "deregister_node",
            "from": node_id,
            "params": {
                "removed_node": N
            }
        })
    except requests.ConnectionError:
        # This happens only if the N node is the leader itself.
        # In this case, we don't need to deregister.
        pass

    # Step 1
    N = NN

    # If we are the only node in the topology, set all pointers to yourself.
    if NN == node_id:
        N = NN = P = L = node_id
        cluster_nodes = []
        return
    # Step 2 - Get new NN pointer.
    NN = (requests.get(f"http://{NN[0]}:{NN[1]}/n").text).split(",")
    # Step 3 - Tell your N to change its P to yourself.
    requests.post(f"http://{N[0]}:{N[1]}", json={
        "msg_type": "change_p",
        "from": node_id,
        "params": {
            "P": node_id
        }
    })
    # Step 4 - Change NN of your P.
    requests.post(f"http://{P[0]}:{P[1]}", json={
        "msg_type": "change_nn",
        "from": node_id,
        "params": {
            "NN": N
        }
    })
    print(f"Fixed N:{N} NN:{NN} L:{L} P:{P}")


def handle_election(params, from_):
    global L, voting
    
    def send_handle_elected(node_):
        try:
            requests.post(f"http://{N[0]}:{N[1]}", json={
                "msg_type": "election",
                "from": node_id,
                "params": {
                    "max_id": node_,
                    "msg_to_retry": params["msg_to_retry"],
                    "sender": params["sender"],
                }
            })
        except requests.ConnectionError:
            print("Start Repairing")
            remove_n_and_repair_topology(params, from_)
            requests.post(f"http://{N[0]}:{N[1]}", json={
                "msg_type": "election",
                "from": node_id,
                "params": {
                    "max_id": node_,
                    "msg_to_retry": params["msg_to_retry"],
                    "sender": params["sender"],
                }
            })

    print((node_id[0],node_id[1]), params["max_id"])
    if (node_id[0],node_id[1]) > (params["max_id"][0],params["max_id"][1]) and voting != True:
        # This happens when the max_id is smaller than node_id, update new leader with this larger node_id
        print("I am larger")
        voting = True
        send_handle_elected(node_id)
    elif (node_id[0],node_id[1]) < (params["max_id"][0],params["max_id"][1]):
        # If node_id of current node is smaller than the max_id in message
        print("I am smaller")
        send_handle_elected(params["max_id"])
    
    else:
        # if election message has gone one round and back to leader
        print(f"Elected is {node_id}")
        L = node_id
        requests.post(f"http://{N[0]}:{N[1]}", json={
            "msg_type": "elected",
            "from": node_id,
            "params": {
                "L": node_id,
                "msg_to_retry": params["msg_to_retry"],
                "sender": params["sender"],
            }
        })
        


def handle_elected(params, from_):
    print("starting elected with ", params["L"])
    global L, voting
    # Resetting to original state
    voting = False
    log_message(params['msg_to_retry'], params['sender'])
    if L != (params["L"][0],params["L"][1]):
        # If L hasn't already been updated – update it.
        L = params["L"]
        if node_id != L:
            print(f"{node_id} sent register to {L}")
            # If this node is not a leader, register yourself to a new leader.
            requests.post(f"http://{L[0]}:{L[1]}", json={
                "msg_type": "register_node",
                "from": node_id,
                "params": {
                    "new_node": node_id
                }
            })
        print("For elected message"+str(get_current_state()))
        print(f"{node_id} sending elected message to {N}")
        requests.post(f"http://{N[0]}:{N[1]}", json={
            "msg_type": "elected",
            "from": node_id,
            "params": {
                "L": L,
                "msg_to_retry": params["msg_to_retry"],
                "sender": params["sender"],
            }
        })
    else:
        print(f'All nodes have updated their leader with {L} using Chang Roberts')


class NodeRequestHandler(BaseHTTPRequestHandler):
    def _set_response_headers(self, content_type='text/html'):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        if self.path == "/n":
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(f"{N[0]},{N[1]}".encode('utf-8'))
            return

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        cur_state = get_current_state()
        self.wfile.write(json.dumps(cur_state).encode('utf-8'))

    def do_POST(self):
        # Read POST body.
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        self._set_response_headers()
        # Unmarshal to dict.
        post_data = json.loads(post_data)
        msg_type = post_data["msg_type"]
        # If value does not exist then use ""
        from_ = post_data.get("from", "")
        params = post_data.get("params", {})
        print(f"Received {msg_type}")
        # Handle message.
        msg_type_to_handler = {
            "join": handle_join,
            "join_reply": handle_join_reply,
            "change_p": handle_change_p,
            "change_nn": handle_change_nn,
            "register_node": handle_register_node,
            "deregister_node": handle_deregister_node,
            "send_chat_msg": handle_send_chat_msg,
            "log_chat_msg": handle_log_chat_msg,
            "dead_node_detected": handle_dead_node_detected,
            "election": handle_election,
            "elected": handle_elected,
        }
        msg_type_to_handler[msg_type](params, from_)


def run(server_class=ThreadingHTTPServer, handler_class=NodeRequestHandler, port=6000, ip="0.0.0.0"):
    server_address = (ip, port)
    httpd = server_class(server_address, handler_class)
    logging.info(f'[NODE STARTED] A node at {httpd.server_address[0]}:{httpd.server_address[1]}.')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info('[NODE STOPPED]')

# Only runs when main function run, not when executed
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ip', '-ip', help="start node with ip", type=str)
    parser.add_argument('--port', '-p', help="start node with port", type=int)
    parser.add_argument('--joinip', '-ji', help="ip of a node to join", type=str)
    parser.add_argument('--joinport', '-jp', help="port of a node to join", type=int)
    parser.add_argument('--nick', '-nick', help="your nickname in the chat", type=str)

    # Parse arguments.
    cli_args = parser.parse_args()
    node_id = (cli_args.ip, cli_args.port)
    joined_node_id = (cli_args.joinip, cli_args.joinport)
    nick = cli_args.nick or "unknown"
    N = NN = P = L = node_id

    if node_id is None and joined_node_id is None:
        parser.error("Not enough arguments.")

    if node_id and cli_args.joinip and cli_args.joinport:
        # Run a thread that joins a cluster after its own server is started.
        threading.Timer(3, join_node, args=[joined_node_id]).start()
    run(port=node_id[1], ip = cli_args.ip)

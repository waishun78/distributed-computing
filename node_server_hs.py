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

node_id = ""
N = NN = P = L = ""
cluster_nodes = []
nick = ""

# Variables for leader election
state = "NOT_INVOLVED"
no_responses = 0
respOK = True
cond = threading.Condition()


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
    requests.post(f"http://0.0.0.0:{joined_node_id}", json={
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
    requests.post(f"http://0.0.0.0:{from_}", json={
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
        requests.post(f"http://0.0.0.0:{P}", json={
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
        requests.post(f"http://0.0.0.0:{prev_N}", json={
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
        requests.post(f"http://0.0.0.0:{L}", json={
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

def initiate_election():
    global state, no_responses, respOK, cond
    state = "CANDIDATE"
    max_depth = 1
    print("Initiating election")

    while state == "CANDIDATE":
        no_responses = 0
        respOK = True
        print("Start sending wave...")
        # Sending NEXT
        try:
            requests.post(f"http://0.0.0.0:{N}", json={
                        "msg_type": "candidature",
                        "from": node_id,
                        "params": {
                            "current_node": node_id,
                            "current_depth": 0,
                            "max_depth": max_depth
                        }
                    })
        except requests.ConnectionError:
            print("Start Repairing")
            handle_dead_node_detected({"dead_node": N}, None)
            requests.post(f"http://0.0.0.0:{N}", json={
                        "msg_type": "candidature",
                        "from": node_id,
                        "params": {
                            "current_node": node_id,
                            "current_depth": 0,
                            "max_depth": max_depth
                        }
                    })
        # Sending PREVIOUS
        try:
            requests.post(f"http://0.0.0.0:{P}", json={
                        "msg_type": "candidature",
                        "from": node_id,
                        "params": {
                            "current_node": node_id,
                            "current_depth": 0,
                            "max_depth": max_depth
                        }
                    })
        except requests.ConnectionError:
            handle_dead_node_detected({"dead_node": P}, None)
            requests.post(f"http://0.0.0.0:{P}", json={
                        "msg_type": "candidature",
                        "from": node_id,
                        "params": {
                            "current_node": node_id,
                            "current_depth": 0,
                            "max_depth": max_depth
                        }
                    })
        # Acquire lock to check no_responses, if not correct, release lock and allow someone else to get lock and then edit value
        print("trying to check for responses after acquiring lock for first time")
        cond.acquire()
        print(f"no_responses {no_responses}")
        while no_responses < 2:
            print("waiting for lock to be notified, blocking")
            val = cond.wait()
            time.sleep(5)
            # Send itself to check again
            print(f"Waiting for responses, have:{no_responses}")
        # Receive both responses back, restart response count
        print("Lock is no longer waiting, unblocked, cond wait fulfilled")
        no_responses = 0
        cond.release()
        # If respOK is False, it is not leader
        if respOK == False:
            state = "LOST"
            print("I give up my candidacy")
            # If respOK is true, then expand reach
        print("increase depth")
        max_depth = max_depth*2

        


# def handle_no_responses(params, from_):
#     global no_responses, respOK, state
#     print("Restart trying")
#     while state == "CANDIDATE":
#         if no_responses < 2:
#             time.sleep(5)
#             # Send itself to check again
#             print(f"Waiting for responses, have:{no_responses}")
#         else:
#             # Receive both responses back, restart response count
#             no_responses = 0
#             # If respOK is False, it is not leader
#             if respOK == False:
#                 state = "LOST"
#             # If respOK is true, then expand reach
#             else:
#                 try:
#                     requests.post(f"http://0.0.0.0:{N}", json={
#                         "msg_type": "candidature",
#                         "from": node_id,
#                         "params": {
#                             "current_node": node_id,
#                             "current_depth": 0,
#                             "max_depth": params["max_depth"]*2,
#                         }
#                     })
#                 except requests.ConnectionError:
#                     print("Start Repairing")
#                     handle_dead_node_detected({"dead_node": N}, None)
#                     requests.post(f"http://0.0.0.0:{N}", json={
#                             "msg_type": "candidature",
#                             "from": node_id,
#                             "params": {
#                                 "current_node": node_id,
#                                 "current_depth": 0,
#                                 "max_depth": params["max_depth"]*2,
#                             }
#                         })
#                 try:
#                     requests.post(f"http://0.0.0.0:{P}", json={
#                             "msg_type": "candidature",
#                             "from": node_id,
#                             "params": {
#                                 "current_node": node_id,
#                                 "current_depth": 0,
#                                 "max_depth": params["max_depth"]*2,
#                             }
#                         })
#                 except requests.ConnectionError:
#                     print("Start Repairing")
#                     handle_dead_node_detected({"dead_node": P}, None)
#                     remove_n_and_repair_topology()
#                     requests.post(f"http://0.0.0.0:{P}", json={
#                             "msg_type": "candidature",
#                             "from": node_id,
#                             "params": {
#                                 "current_node": node_id,
#                                 "current_depth": 0,
#                                 "max_depth": params["max_depth"]*2,
#                             }
#                         })

        
def handle_send_chat_msg(params, from_):
    sender = params.get("sender", f"{node_id}, {nick}")

    if node_id == L:
        log_message(params['chat_msg'], sender)
        unreachable_node = None
        for node in cluster_nodes:
            try:
                requests.post(f"http://0.0.0.0:{node}", json={
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
                remove_n_and_repair_topology()
            else:
                requests.post(f"http://0.0.0.0:{N}", json={
                    "msg_type": "dead_node_detected",
                    "from": node_id,
                    "params": {
                        "dead_node": unreachable_node
                    }
                })
    elif node_id != L:
        try:
            requests.post(f"http://0.0.0.0:{L}", json={
                "msg_type": "send_chat_msg",
                "from": node_id,
                "params": {
                    "chat_msg": params["chat_msg"],
                    "sender": sender
                }
            })
        except requests.ConnectionError:
            print("Starting a new election")
            initiate_election()


def handle_dead_node_detected(params, from_):
    dead_node = params["dead_node"]
    if N == dead_node:
        # print(f"Repairing on {node_id}")
        remove_n_and_repair_topology()
    else:
        requests.post(f"http://0.0.0.0:{N}", json={
            "msg_type": "dead_node_detected",
            "from": node_id,
            "params": {
                "dead_node": dead_node
            }
        })


def remove_n_and_repair_topology():
    global N, NN, P, L, cluster_nodes
    # Step 0 - Deregister N node from a leader.
    try:
        requests.post(f"http://0.0.0.0:{L}", json={
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
    NN = int(requests.get(f"http://0.0.0.0:{NN}/n").text)
    # Step 3 - Tell your N to change its P to yourself.
    requests.post(f"http://0.0.0.0:{N}", json={
        "msg_type": "change_p",
        "from": node_id,
        "params": {
            "P": node_id
        }
    })
    # Step 4 - Change NN of your P.
    requests.post(f"http://0.0.0.0:{P}", json={
        "msg_type": "change_nn",
        "from": node_id,
        "params": {
            "NN": N
        }
    })

def handle_candidature(params, from_):
    global N, P, state
    print(f"Handling candidacy from {params['current_node']}")
    if params["current_node"] < node_id:
        print(f"I am a better candidate, my state is {state}")
        if from_ == P:
            requests.post(f"http://0.0.0.0:{P}", json={
                    "msg_type": "candidature_response",
                    "from": node_id,
                    "params": {
                        "is_leader": False,
                        "node_q": params["current_node"]
                    }
                })
        else:
            requests.post(f"http://0.0.0.0:{N}", json={
                    "msg_type": "candidature_response",
                    "from": node_id,
                    "params": {
                        "is_leader": False,
                        "node_q": params["current_node"]
                    }
                })
        if state == "NOT_INVOLVED":
            initiate_election()
    elif params["current_node"] > node_id:
        state = "LOST"
        print("Lost")
        depth = params["current_depth"] + 1
        if depth < params["max_depth"]:
            # Did not meet max depth
            # Pass on message forward
            # If can from previous node:
            print("Havent meet depth")
            if from_ == P:
                try:
                    print(f"I am node {node_id} passing on the message to {N}")
                    requests.post(f"http://0.0.0.0:{N}", json={
                        "msg_type": "candidature",
                        "from": node_id,
                        "params": {
                            "current_node": params["current_node"],
                            "current_depth": depth,
                            "max_depth": params["max_depth"]
                        }
                    })
                except requests.ConnectionError:
                    print("Start Repairing")
                    handle_dead_node_detected({"dead_node": N}, None)
                    remove_n_and_repair_topology()
                    requests.post(f"http://0.0.0.0:{N}", json={
                        "msg_type": "candidature",
                        "from": node_id,
                        "params": {
                            "current_node": params["current_node"],
                            "current_depth": depth,
                            "max_depth": params["max_depth"]
                        }
                    })
            else:
                try:
                    print(f"I am node {node_id} passing on the message to {P}")
                    requests.post(f"http://0.0.0.0:{P}", json={
                        "msg_type": "candidature",
                        "from": node_id,
                        "params": {
                            "current_node": params["current_node"],
                            "current_depth": depth,
                            "max_depth": params["max_depth"]
                        }
                    })
                except requests.ConnectionError:
                    print("Start Repairing")
                    handle_dead_node_detected({"dead_node": P}, None)
                    remove_n_and_repair_topology()
                    requests.post(f"http://0.0.0.0:{P}", json={
                        "msg_type": "candidature",
                        "from": node_id,
                        "params": {
                            "current_node": params["current_node"],
                            "current_depth": depth,
                            "max_depth": params["max_depth"]
                        }
                    })
        else:
            print("Meet max depth, respond")
            # Meet max depth
            # Reply candidate
            
            if from_ == P:
                print(f"I am node {node_id} passing on the message to {P} for {params['current_node']}")
                requests.post(f"http://0.0.0.0:{P}", json={
                        "msg_type": "candidature_response",
                        "from": node_id,
                        "params": {
                            "is_leader": True,
                            "node_q": params["current_node"]
                        }
                    })
            else:
                print(f"I am node {node_id} passing on the message to {N} for {params['current_node']}")
                requests.post(f"http://0.0.0.0:{N}", json={
                        "msg_type": "candidature_response",
                        "from": node_id,
                        "params": {
                            "is_leader": True,
                            "node_q": params["current_node"]
                        }
                    })
    else:
        # Receive returning message
        print("Elected")
        if state != "ELECTED":
            state = "ELECTED"
            winner = node_id
            handle_elected({"L": node_id}, None)

def handle_candidature_response(params, from_):
    global no_responses, respOK, cond
    print(f"Handling candidature response by candidate {params['node_q']}")
    if params["node_q"] == node_id:
        # Reply belongs to current node
        print("Trying to acquire lock")
        cond.acquire()
        no_responses +=1
        print(f"Update no_responses to {no_responses}")
        respOK = respOK and params["is_leader"]
        cond.release()
        print(f"Update respOK to {respOK}")
        print(f"Message causing me to update is from {from_}")
        if no_responses == 2:
            print("acquiring to notify")
            cond.acquire()
            print("notifying all")
            cond.notify()
            cond.release()

    else:
        if from_ == P:
            print(f"I am node{node_id} and I am passing candidature response for {params['node_q']} to {N}")
            requests.post(f"http://0.0.0.0:{N}", json={
                    "msg_type": "candidature_response",
                    "from": node_id,
                    "params": {
                        "is_leader": params["is_leader"],
                        "node_q": params["node_q"]
                    }
                })
        else:
            print(f"I am node{node_id} and I am passing candidature response for {params['node_q']} to {P}")
            requests.post(f"http://0.0.0.0:{P}", json={
                    "msg_type": "candidature_response",
                    "from": node_id,
                    "params": {
                        "is_leader": params["is_leader"],
                        "node_q": params["node_q"]
                    }
                })
            

def handle_elected(params, from_):
    # print("starting elected with ", params["L"])
    global L, state, respOK
    if L != params["L"]:
        # If L hasn't already been updated – update it.
        L = params["L"]
        # Resetting states
        state = "NOT_INVOLVED"
        no_responses = 0
        respOK = True
        if node_id != L:
            # print(f"{node_id} sent register to {L}")
            # If this node is not a leader, register yourself to a new leader.
            requests.post(f"http://0.0.0.0:{L}", json={
                "msg_type": "register_node",
                "from": node_id,
                "params": {
                    "new_node": node_id
                }
            })
        requests.post(f"http://0.0.0.0:{N}", json={
            "msg_type": "elected",
            "from": node_id,
            "params": {
                "L": L,
            }
        })
    else:
        print(f'All nodes have updated their leader with {L}')


class NodeRequestHandler(BaseHTTPRequestHandler):
    def _set_response_headers(self, content_type='text/html'):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        if self.path == "/n":
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(f"{N}".encode('utf-8'))
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
            # "election": handle_election,
            "elected": handle_elected,
            "candidature": handle_candidature,
            "candidature_response": handle_candidature_response
        }
        msg_type_to_handler[msg_type](params, from_)


def run(server_class=ThreadingHTTPServer, handler_class=NodeRequestHandler, port=6000):
    server_address = ('0.0.0.0', port)
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
    parser.add_argument('--port', '-p', help="start node with port", type=int)
    parser.add_argument('--join', '-join', help="port of a node to join", type=int)
    parser.add_argument('--nick', '-nick', help="your nickname in the chat", type=str)

    # Parse arguments.
    cli_args = parser.parse_args()
    node_id = cli_args.port
    joined_node_id = cli_args.join
    nick = cli_args.nick or "unknown"
    N = NN = P = L = node_id

    if node_id is None and joined_node_id is None:
        parser.error("Not enough arguments.")

    if node_id and joined_node_id:
        # Run a thread that joins a cluster after its own server is started.
        threading.Timer(3, join_node, args=[joined_node_id]).start()
    run(port=node_id)

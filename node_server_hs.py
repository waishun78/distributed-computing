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

# Variables for leader election
state = "NOT_INVOLVED"
no_responses = 0
respOK = True
cond = threading.Condition()
condRemoved = threading.Condition()
removed = False

# Make sure message of elected arrive on both sides
elected_msg_count = 0
condBothMsg = threading.Condition()

last_repaired_node = ("","")
condRepair = threading.Condition()



def log_message(msg, sender, color=""):
    logging.info(f"[{sender}]: {msg}\n")


def get_current_state():
    return {
        "Node": node_id,
        "N": N,
        "NN": NN,
        "P": P,
        "L": L,
        "cluster_nodes": cluster_nodes,
        # "state": state,
        # "no_responses": no_responses,
        # "respOK": respOK
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
    N = (from_[0],from_[1])
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
    if (node_id[0],node_id[1]) == (L[0],L[1]):
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
    P = (params["P"][0],params["P"][1])


def handle_change_nn(params, from_):
    global NN
    NN = (params["NN"][0],params["NN"][1])


def handle_join_reply(params, from_):
    global N, NN, L
    N = (params["N"][0],params["N"][1])
    NN = (params["NN"][0],params["NN"][1])
    L = (params["L"][0],params["L"][1])


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
        try:
            requests.post(f"http://{N[0]}:{N[1]}", json={
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
            handle_dead_node_detected({"dead_node": N, "node_who_found_dead": node_id}, None)
            requests.post(f"http://{N[0]}:{N[1]}", json={
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
            requests.post(f"http://{P[0]}:{P[1]}", json={
                        "msg_type": "candidature",
                        "from": node_id,
                        "params": {
                            "current_node": node_id,
                            "current_depth": 0,
                            "max_depth": max_depth
                        }
                    })
        except requests.ConnectionError:
            handle_dead_node_detected({"dead_node": P,  "node_who_found_dead": node_id}, None)
            requests.post(f"http://{P[0]}:{P[1]}", json={
                        "msg_type": "candidature",
                        "from": node_id,
                        "params": {
                            "current_node": node_id,
                            "current_depth": 0,
                            "max_depth": max_depth
                        }
                    })
        # Acquire lock to check no_responses, if not 2 yet, release lock and allow handle_candidature_response 
        # process to update no_responses and respOK var
        print("trying to check for responses after acquiring lock for first time")
        cond.acquire()
        print(f"no_responses {no_responses}")
        while no_responses < 2:
            print("waiting for lock to be notified, blocking")
            val = cond.wait()
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
        # Increase max_depth by two, if node is still a candidate, it will send next wave of candidature messages
        print("increase depth")
        max_depth = max_depth*2
        
def handle_send_chat_msg(params, from_):
    global state, L, condRemoved, removed
    if state == "RESETTING":
        state = "NOT_INVOLVED"
    sender = params.get("sender", f"{node_id}, {nick}")

    if (node_id[0],node_id[1]) == (L[0],L[1]):
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
            if (unreachable_node[0],unreachable_node[1]) == (N[0],N[1]):
                remove_n_and_repair_topology(node_id)
            else:
                requests.post(f"http://{N[0]}:{N[1]}", json={
                    "msg_type": "dead_node_detected",
                    "from": node_id,
                    "params": {
                        "dead_node": unreachable_node
                    }
                })
    elif (node_id[0],node_id[1]) != (L[0],L[1]):
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
            # Since the leader is the node that has failed
            # TODO: need to make sure topology is corrected before initiating election
            handle_dead_node_detected({"dead_node": L, "node_who_found_dead": node_id}, None)
            print("found dead, acquire lock to check if removed")
            condRemoved.acquire()
            if removed == False:
                print("Waiting for removal to finish")
                val = condRemoved.wait()
            removed = False
            print("Removal is done and election is started")
            condRemoved.release()
            initiate_election()

def handle_notify_removal(params, from_):
    global removed
    condRemoved.acquire()
    removed = True
    condRemoved.notify()
    print("Notify Remove Lock")
    condRemoved.release()


def handle_dead_node_detected(params, from_):
    global last_repaired_node, condRepair
    condRepair.acquire()
    dead_node = params["dead_node"]
    if (N[0],N[1]) == (dead_node[0], dead_node[1]):
        print(f"Repairing on {node_id}")
        last_repaired_node = (dead_node[0], dead_node[1])
        remove_n_and_repair_topology(params["node_who_found_dead"])
    else:
        # Check if it is already repaired, cause if it is already repaired, it might keep passing this message and cause a loop when it is alr fixed
        if (dead_node[0], dead_node[1]) != (last_repaired_node[0],last_repaired_node[1]): 
            requests.post(f"http://{N[0]}:{N[1]}", json={
                "msg_type": "dead_node_detected",
                "from": node_id,
                "params": {
                    "dead_node": dead_node,
                    "node_who_found_dead": params["node_who_found_dead"]
                }
            })
    condRepair.release()

def remove_n_and_repair_topology(node_to_notify):
    global N, NN, P, L, cluster_nodes, condRemoved, removed
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
    if (NN[0],NN[1]) == (node_id[0],node_id[1]):
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
    requests.post(f"http://{node_to_notify[0]}:{node_to_notify[1]}", json={
        "msg_type": "notify_removal",
        "from": node_id,
        "params": {
            "removal": "True"
        }
    })


def handle_candidature(params, from_):
    global N, P, state, condBothMsg, elected_msg_count
    # print(f"Handling candidature from {params['current_node']}")
    # print(node_id)
    # print(params["current_node"][0],params["current_node"][1])
    # print((params["current_node"][0],params["current_node"][1]) < node_id)
    if (params["current_node"][0],params["current_node"][1]) < (node_id[0],node_id[1]):
        print(f"I am a better candidate, my state is {state}")
        if (from_[0],from_[1]) == (P[0],P[1]):
            requests.post(f"http://{P[0]}:{P[1]}", json={
                    "msg_type": "candidature_response",
                    "from": node_id,
                    "params": {
                        "is_leader": False,
                        "node_q": params["current_node"]
                    }
                })
        else:
            requests.post(f"http://{N[0]}:{N[1]}", json={
                    "msg_type": "candidature_response",
                    "from": node_id,
                    "params": {
                        "is_leader": False,
                        "node_q": params["current_node"]
                    }
                })
        if state == "NOT_INVOLVED":
            initiate_election()
    elif (params["current_node"][0],params["current_node"][1]) > (node_id[0],node_id[1]):
        if state != "NOT_INVOLVED":
            state = "LOST"
            print("Lost")
        depth = params["current_depth"] + 1
        if depth < params["max_depth"]:
            # Did not meet max depth
            # Pass on message to the node in the original direction of message sent
            print("Havent meet depth")
            if (from_[0],from_[1]) == (P[0],P[1]):
                try:
                    print(f"I am node {node_id} passing on the candidature to {N} for {params['current_node']}")
                    requests.post(f"http://{N[0]}:{N[1]}", json={
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
                    handle_dead_node_detected({"dead_node": N, "node_who_found_dead": node_id}, None)
                    remove_n_and_repair_topology(node_id)
                    requests.post(f"http://{N[0]}:{N[1]}", json={
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
                    print(f"I am node {node_id} passing on the candidature to {P} for {params['current_node']}")
                    requests.post(f"http://{P[0]}:{P[1]}", json={
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
                    handle_dead_node_detected({"dead_node": P, "node_who_found_dead": node_id}, None)
                    remove_n_and_repair_topology(node_id)
                    requests.post(f"http://{P[0]}:{P[1]}", json={
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
            
            if (from_[0],from_[1]) == (P[0],P[1]):
                print(f"I am node {node_id} passing on the candidature to {P} for {params['current_node']}")
                requests.post(f"http://{P[0]}:{P[1]}", json={
                        "msg_type": "candidature_response",
                        "from": node_id,
                        "params": {
                            "is_leader": True,
                            "node_q": params["current_node"]
                        }
                    })
            else:
                print(f"I am node {node_id} passing on the candidature to {N} for {params['current_node']}")
                requests.post(f"http://{N[0]}:{N[1]}", json={
                        "msg_type": "candidature_response",
                        "from": node_id,
                        "params": {
                            "is_leader": True,
                            "node_q": params["current_node"]
                        }
                    })
    else:
        # Receive returning message
        print("Treying to acquire condBothMsg lock")
        condBothMsg.acquire()
        if elected_msg_count == 0:
            print("receive one candidature that went one round, waiting for the other one")
            elected_msg_count += 1
        else:
            print("Elected")
            if state != "ELECTED":
                state = "ELECTED"
                winner = node_id
                handle_elected({"L": node_id}, None)
            elected_msg_count = 0
        condBothMsg.release()
        print("release condBothMsg lock")

def handle_candidature_response(params, from_):
    global no_responses, respOK, cond
    print(f"Handling candidature response by candidate {params['node_q']}")
    if (params["node_q"][0],params["node_q"][1])== (node_id[0],node_id[1]):
        # Reply belongs to current node
        print("Trying to acquire lock")
        cond.acquire()
        no_responses +=1
        print(f"Update no_responses to {no_responses}")
        respOK = respOK and params["is_leader"]
        print(f"Update respOK to {respOK}")
        print(f"Message causing me to update is from {from_}")
        if no_responses == 2:
            print("acquiring to notify")
            print("notifying all")
            cond.notify()
        cond.release()

    else:
        if (from_[0],from_[1]) == (P[0],P[1]):
            print(f"I am node{node_id} and I am passing candidature response for {params['node_q']} to {N}")
            requests.post(f"http://{N[0]}:{N[1]}", json={
                    "msg_type": "candidature_response",
                    "from": node_id,
                    "params": {
                        "is_leader": params["is_leader"],
                        "node_q": params["node_q"]
                    }
                })
        else:
            print(f"I am node{node_id} and I am passing candidature response for {params['node_q']} to {P}")
            requests.post(f"http://{P[0]}:{P[1]}", json={
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
    if (L[0],L[1]) != (params["L"][0],params["L"][1]):
        # If L hasn't already been updated – update it.
        L = (params["L"][0],params["L"][1])
        print(f"Updating elected to {L}")
        # Resetting states, sleep to ensure all messages come in time
        time.sleep(1)
        state = "RESETTING"
        no_responses = 0
        respOK = True
        if (node_id[0],node_id[1]) != (L[0],L[1]):
            # print(f"{node_id} sent register to {L}")
            # If this node is not a leader, register yourself to a new leader.
            requests.post(f"http://{L[0]}:{L[1]}", json={
                "msg_type": "register_node",
                "from": node_id,
                "params": {
                    "new_node": node_id
                }
            })
        requests.post(f"http://{N[0]}:{N[1]}", json={
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
            self.wfile.write(f"{N[0]},{N[1]}".encode('utf-8'))
            return

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        cur_state = get_current_state()
        self.wfile.write(json.dumps(cur_state).encode('utf-8'))

    def do_POST(self):
        # Read POST body.
        time.sleep(2)
        print(get_current_state())
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        self._set_response_headers()
        # Unmarshal to dict.
        post_data = json.loads(post_data)
        print(post_data)
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
            "candidature_response": handle_candidature_response,
            "notify_removal": handle_notify_removal
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

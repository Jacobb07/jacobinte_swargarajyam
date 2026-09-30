import asyncio, websockets
import json, uuid, base64
import threading, socket
import os, subprocess
from typing import Set, Dict, List, Tuple
from consensus.pow.blockchain_structures import Transaction, Block, Wallet, Chain, isvalidChain, get_tx_amount
from ipfs.ipfs import addToIpfs, download_ipfs_file_subprocess
from smart_contract.contracts_db import SmartContractDatabase
from smart_contract.secure_executor import SecureContractExecutor
from storage.storage_manager import save_key, load_key, save_chain, load_chain, save_peers, load_peers
from ecdsa import VerifyingKey
from pathlib import Path
import tempfile
import ast 
import hashlib
import math
import shutil

MAX_CONNECTIONS = 8
GAS_PRICE = 0.001 # coin per gas unit
BASE_DEPLOY_COST = 5
CONSENSUS ="pow"

def get_random_element(s):
    """
        Return a random element from a set
    """
    import random
    return random.choice(list(s)) if s else None

def normalize_endpoint(ep):
    """
        Return host resolved into ipv4 address and port converted into int datatype - maintains consistency in the code
    """
    host, port = ep
    return (socket.gethostbyname(host), int(port))

def get_contract_code_from_notepad():
    """Opens a temp .py file in a text editor that exists on this OS and returns what the user wrote."""
    template = (
        "# Write your smart contract function here.\n"
        "def contract_logic(parameter1, parameter2, parameter3, state):\n"
        "    # your code here\n"
        "    return state, 'some message'\n"
    )
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode='w+', encoding='utf-8') as tmp_file:
        temp_filename = tmp_file.name
        tmp_file.write(template)

    # The old code always ran "gedit", which does not exist on Windows / macOS.
    if os.name == "nt":
        editor = ["notepad"]
    elif shutil.which(os.environ.get("EDITOR", "")) if os.environ.get("EDITOR") else False:
        editor = [os.environ["EDITOR"]]
    elif shutil.which("gedit"):
        editor = ["gedit"]
    elif shutil.which("nano"):
        editor = ["nano"]
    elif os.uname().sysname == "Darwin":
        editor = ["open", "-W", "-t"]
    else:
        editor = ["vi"]

    try:
        subprocess.call(editor + [temp_filename])
        with open(temp_filename, 'r', encoding='utf-8') as f:
            contract_code = f.read()
    finally:
        os.remove(temp_filename)

    if contract_code.strip() == template.strip():   # user saved without writing anything
        return ""
    return contract_code

class Peer:
    def __init__(self, host, port, name, miner:bool, activate_disk_load, activate_disk_save):
        self.host = host
        self.name = name
        self.miner=miner

        self.activate_disk_save = activate_disk_save

        self.port = port
        self.ipfs_port = port + 50  # API port
        self.gateway_port = port + 81  # Gateway port
        self.swarm_tcp = port + 2
        self.swarm_udp = port + 3
        self.repo_path = Path.home() / f".ipfs_{port}"
        self.env = os.environ.copy()
        self.env["IPFS_PATH"] = str(self.repo_path)

        self.server_connections :Set[websockets.WebSocketServerProtocol]=set() # For inbound peers ie websockets that connect to us and treat us as the server
        self.client_connections :Set[websockets.WebSocketServerProtocol]=set() # For outbound peers ie websockets we initiated, we are the clients

        self.outbound_peers: Set[tuple]=set()
        # The peers to which we currently maintain a outbound connection

        self.seen_message_ids: Set[str]= set()
        # Used to remove duplicate messages, messages that return to us after a round of broadcasting

        self.name_to_public_key_dict: Dict[str, str]={}   # must exist BEFORE peers are loaded from disk
        if activate_disk_load == "y":
            self.load_known_peers_from_disk()
        else:
            self.known_peers = None
        if not self.known_peers:
            self.known_peers : Dict[Tuple[str, int], Tuple[str, str]]={} # (host, port):(name, public key)
        """
            We store all the peers we know here, we compare this with outbound peers in dicover_peers
            to find to which nodes we have not yet made a connection
        """
        

        self.got_pong: Dict[websockets.WebSocketServerProtocol, bool]={}
        """
            We set the value of each websocket in this dictionary false before sending ping
            If we get a pong from a particular websocekt we assign it True.
            We remove all websockets that don't send a pong in time. 
        """

        self.have_sent_peer_info: Dict[websockets.WebSocketServerProtocol, bool]={}
        """
            When we form an outbound connection, on receiving the first pong after our first ping
            we send them our peer_info, but we don't want to keep making the elaborate handshake
            so after the first time of getting pong we don't send them our peer info
            I'll explain the handshake in README.md
        """

        self.mem_pool: List[Transaction]=list()

        self.file_hashes: Dict[str, str]={}
        self.file_hashes_lock= asyncio.Lock()
        self.daemon_process=None

        if activate_disk_load == "y":
            self.load_key_from_disk()
        else:
            self.wallet = None
        if not self.wallet:
            self.wallet=Wallet()
            if self.activate_disk_save == "y":
                self.save_key_to_disk()
        
        if activate_disk_load == "y":
            self.load_chain_from_disk() # If no chain data stored, self.chain will be assigned to None
        else:
            self.chain = None

        self.contractsDB = SmartContractDatabase()

        self.mem_pool_condition=asyncio.Condition() 
        """
            Any block under this condition must acquire lock before moving on so we don't need to use both at the same time
            A condition is a synchronous primitive with an inbuilt lock
            This means that while a block of code starting with
            async with self.mem_pool_condition is executed only if
            there is no other such block currently being executed
        """
        self.mine_task=None
        self.mine_stop=None

    def save_key_to_disk(self):
        key = self.wallet.private_key_pem
        save_key(key, CONSENSUS)

    def load_key_from_disk(self):
        key = load_key(CONSENSUS)
        if not key:
            self.wallet = None
            return
        self.wallet = Wallet(key)

    def load_chain_from_disk(self):
        block_dict_list = load_chain(CONSENSUS)
        if not block_dict_list:
            self.chain = None
            return
        block_list: List[Block]=[]

        for block_dict in block_dict_list:
            block=self.block_dict_to_block(block_dict)
            block_list.append(block)

        self.chain=Chain(blockList=block_list)

    def save_chain_to_disk(self):
        chain = Chain.instance.to_block_dict_list()
        save_chain(chain, CONSENSUS)

    def save_known_peers_to_disk(self):
        content = {}
        for key, value in self.known_peers.items():
            content[json.dumps(key)] = list(value)
        save_peers(content, CONSENSUS)

    def load_known_peers_from_disk(self):
        content = load_peers(CONSENSUS)
        if not content:
            self.known_peers = None
            return
        self.known_peers = {}
        for key, value in content.items():
            self.known_peers[tuple(ast.literal_eval(key))] = tuple(value)
        for key, value in self.known_peers.items():
            self.name_to_public_key_dict[value[0].lower()] = value[1]

    async def send_peer_info(self, websocket):
        """
            Function made to send the peer (self) info
        """
        pkt={
            "type":"peer_info",
            "id":str(uuid.uuid4()),
            "data":{
                "host":self.host,
                "port":self.port,
                "name":self.name,
                "public_key":self.wallet.public_key_pem
                }
        }

        self.seen_message_ids.add(pkt["id"])
        await websocket.send(json.dumps(pkt))

    async def send_known_peers(self, websocket):
        """
            Function for sending known_peers
            (information regarding all the peers we know)
        """
        peers=[{"host":h, "port":p, "name":n, "public_key":s}
               for (h, p), (n,s) in self.known_peers.items()]
        peers.append({"host":self.host, "port":self.port, "name":self.name, "public_key":self.wallet.public_key_pem})
        pkt={
            "type":"known_peers",
            "id":str(uuid.uuid4()),
            "peers":peers
        }
        self.seen_message_ids.add(pkt["id"])
        await websocket.send(json.dumps(pkt))

    def block_dict_to_block(self, block_dict):    
        """
            This function creates a block out of the information
            stored inside block_dict
            We sent a receive blocks as a dictionary
            block["trasnsactions"] is a list of dictionaries that
            represent transactions
        """

        new_block_id=block_dict["id"]
        new_block_prevHash=block_dict["prevHash"]
        new_block_ts=block_dict["ts"]
        new_block_nonce=block_dict["nonce"]

        transactions=[]
        for transaction_dict in block_dict["transactions"]:
            transaction=Transaction(transaction_dict["payload"], transaction_dict["sender"], transaction_dict["receiver"], transaction_dict["id"], transaction_dict["ts"])
            if(transaction.sender!="Genesis"):
                transaction.sign=base64.b64decode(transaction_dict["sign"])
            transactions.append(transaction)

        
        newBlock=Block(new_block_prevHash, transactions, new_block_ts, new_block_nonce, new_block_id)   
        newBlock.files=block_dict.get("files", {})
        newBlock.miner=block_dict.get("miner")

        return newBlock

    def valid_deploy_transaction(self, payload):
        try:
            if len(payload) != 2 or not isinstance(payload[0], str):
                return False
            gas_used = len(payload[0])//10 + BASE_DEPLOY_COST
            return math.isclose(gas_used * GAS_PRICE, payload[-1])
        except Exception:
            return False

    def valid_invoke_transaction(self, payload):
        """Blocking (runs the contract in a sandbox process) - call it with asyncio.to_thread."""
        try:
            if len(payload) != 5 or not isinstance(payload[2], list):
                return False
            response = self.run_contract([payload[0], payload[1], payload[2]])
            if response["error"] is not None:
                return False
            amount = response["gas_used"] * GAS_PRICE
            return response["state"] == payload[3] and math.isclose(amount, payload[-1])
        except Exception as e:
            print(f"Invoke validation failed: {e}")
            return False

    def get_unique_name(self, base_name):
        existing_names = []
        for key, value in self.known_peers.items():
            existing_names.append(value[0].lower())

        existing_names.append(self.name.lower())
        
        base_name = base_name.lower()
        if base_name not in existing_names:
            return base_name
    
        counter = 1
        while True:
            new_name = f"{base_name}{counter}"
            if new_name not in existing_names:
                return new_name
            counter += 1

    async def handle_messages(self, websocket, msg):
        """One bad message must never kill the connection loop, so errors are contained here."""
        try:
            await self._handle_messages_inner(websocket, msg)
        except Exception as e:
            print(f"Ignoring bad message ({type(e).__name__}: {e})")

    async def _handle_messages_inner(self, websocket, msg):
        """
            This is a function to handle messages as the name suggests
            It faciliates the handshake between client and server. 
            It also accepts transactions, blocks and chains in the format
            in which we send them. Then this function recreates these and
            performs the necessary operations
        """
        # Read the handshake protocol within readme to understand the flow of messages

        t=msg.get("type")
        id=msg.get("id")
        # Every message has a type and an id

        if not t or not id:
            return
        if id in self.seen_message_ids:
            return
        
        self.seen_message_ids.add(id)

        if t=="ping":
            pkt={
                "type":"pong",
                "id":str(uuid.uuid4())
                }
            self.seen_message_ids.add(pkt["id"])
            await websocket.send(json.dumps(pkt))

        elif t=="pong":
            self.got_pong[websocket]=True
            if not self.have_sent_peer_info.get(websocket, True):
                await self.send_peer_info(websocket)
                self.have_sent_peer_info[websocket]=True

        elif t =='peer_info':
            data=msg["data"]
            normalized_self=normalize_endpoint((self.host, self.port))
            normalized_endpoint = normalize_endpoint((data['host'], data['port']))
            if normalized_endpoint not in self.known_peers and normalized_endpoint!=normalized_self :
                self.known_peers[normalized_endpoint]=(data['name'], data['public_key'])
                if self.activate_disk_save == "y":
                    self.save_known_peers_to_disk()
                self.name_to_public_key_dict[data['name'].lower()]=data['public_key']
                print(f"Registered peer {data['name']} {data['host']}:{data['port']}")
                await self.send_known_peers(websocket)

        elif t == "add_peer":
            data=msg["data"]
            normalized_self=normalize_endpoint((self.host, self.port))
            normalized_endpoint = normalize_endpoint((data["host"], data["port"]))
            new_peer_msg_id = str(uuid.uuid4())
            if normalized_endpoint not in self.known_peers and normalized_endpoint!=normalized_self :
                proposed_name = self.get_unique_name(data["name"])
                if proposed_name != data["name"].lower():
                    pkt={
                        "type":"change_name",
                        "id":str(uuid.uuid4()),
                        "new_peer_msg_id": new_peer_msg_id,
                        "new_name": proposed_name
                    }
                    await websocket.send(json.dumps(pkt))
                    data["name"] = proposed_name
                self.known_peers[normalized_endpoint]=(data["name"], data["public_key"])
                if self.activate_disk_save == "y":
                    self.save_known_peers_to_disk()
                self.name_to_public_key_dict[data["name"].lower()]=data["public_key"]
                print(f"Registered peer {data['name']} {data['host']}:{data['port']}")
                await self.send_known_peers(websocket)
                pkt={
                    "type":"new_peer",
                    "id":new_peer_msg_id,
                    "data":{
                        "host":data["host"],
                        "port":data["port"],
                        "name":data["name"],
                        "public_key":data["public_key"]
                    }
                }
                self.seen_message_ids.add(pkt["id"])
                await self.broadcast_message(pkt)

        elif t=="new_peer":
            data=msg["data"]
            normalized_self=normalize_endpoint((self.host, self.port))
            normalized_endpoint = normalize_endpoint((data["host"], data["port"]))
            if normalized_endpoint not in self.known_peers and normalized_endpoint!=normalized_self :
                self.known_peers[normalized_endpoint]=(data["name"], data["public_key"])
                if self.activate_disk_save == "y":
                    self.save_known_peers_to_disk()
                self.name_to_public_key_dict[data["name"].lower()]=data["public_key"]
                print(f"Registered peer {data['name']} {data['host']}:{data['port']}")
                await self.broadcast_message(msg)

        elif t=="change_name":
            new_name = msg["new_name"]
            self.name = new_name
            self.seen_message_ids.add(msg["new_peer_msg_id"])

        elif t=="known_peers":
            peers=msg["peers"]
            new_peer_found = False
            for peer in peers:
                normalized_self=normalize_endpoint((self.host, self.port))
                normalized_endpoint = normalize_endpoint((peer["host"], peer["port"]))
                if normalized_endpoint not in self.known_peers and normalized_endpoint!=normalized_self:
                    print(f"Discovered peer {peer['name']} at {peer['host']}:{peer['port']}")
                    new_peer_found = True
                    self.known_peers[normalized_endpoint]=(peer["name"], peer["public_key"])
                    self.name_to_public_key_dict[peer["name"].lower()]=peer["public_key"]
            if new_peer_found:
                if self.activate_disk_save == "y":
                    self.save_known_peers_to_disk()
            pkt={
                "type":"chain_request",
                "id":str(uuid.uuid4())
            }
            await websocket.send(json.dumps(pkt))

        elif t=="file":
            cid=msg["cid"]
            desc=msg["desc"]
            async with self.file_hashes_lock:
                self.file_hashes[cid]=desc

            await self.broadcast_message(msg)

        elif t=="new_tx":
            if Chain.instance is None:
                return  # We have no chain yet, so we cannot judge transactions
            try:
                tx_str=msg["transaction"]
                tx=json.loads(tx_str)
                transaction=Transaction(tx['payload'], tx['sender'], tx['receiver'], tx['id'], tx['ts'])
                sign_bytes=base64.b64decode(msg["sign"])
                # Signature first: it is cheap, and it stops strangers from making us run contracts
                VerifyingKey.from_pem(tx['sender'].encode()).verify(sign_bytes, tx_str.encode())
            except Exception:
                print("Invalid Signature or malformed transaction")
                return

            if Chain.instance.transaction_exists_in_chain(transaction):
                print(f"{self.name} Transaction already exists in chain")
                return
            if any(pooled.id==transaction.id for pooled in self.mem_pool):
                return

            amount=get_tx_amount(transaction)
            if amount is None or amount<=0:
                print("\nInvalid Transaction, bad amount\n")
                return

            if transaction.receiver == "deploy" and not self.valid_deploy_transaction(transaction.payload):
                return
            if transaction.receiver == "invoke" and not await asyncio.to_thread(self.valid_invoke_transaction, transaction.payload):
                return

            if amount>Chain.instance.calc_balance(transaction.sender, self.mem_pool):
                print("\nAttempt to spend more than one has, Invalid transaction\n")
                return

            transaction.sign=sign_bytes

            print("\nValid Transaction")
            print(f"\n{msg['type']}: {msg['transaction']}")
            print("\n")

            async with self.mem_pool_condition:
                self.mem_pool.append(transaction)
            await self.broadcast_message(msg)

        elif t=="new_block":
            if Chain.instance is None:
                return
            try:
                newBlock=self.block_dict_to_block(msg["block"])
            except Exception:
                print("\nMalformed Block\n")
                return

            if not Chain.instance.isValidBlock(newBlock):
                print("\nInvalid Block\n")
                return

            for transaction in newBlock.transactions:
                if transaction.receiver == "invoke":
                    if not await asyncio.to_thread(self.valid_invoke_transaction, transaction.payload):
                        return
                if transaction.receiver == "deploy":
                    if not self.valid_deploy_transaction(transaction.payload):
                        return

            # newBlock.miner comes from the block itself and is covered by its proof of work
            Chain.instance.chain.append(newBlock)
            print("\n\n Block Appended \n\n")

            for transaction in newBlock.transactions:
                if transaction.receiver == "deploy":
                    self.deploy_contract(transaction)

            if self.miner:
                self._abort_mining()
                print("New Block received Cancelled Mining...")

            async with self.mem_pool_condition:
                # Build a new list: removing from a list while looping over it skips items
                self.mem_pool[:]=[tx for tx in self.mem_pool if not newBlock.transaction_exists_in_block(tx)]

            async with self.file_hashes_lock:
                for hash in list(self.file_hashes.keys()):
                    if newBlock.cid_exists_in_block(hash):
                        self.file_hashes.pop(hash, None)

            if self.miner:
                self.mine_task=asyncio.create_task(self.mine_blocks())
            await self.broadcast_message(msg)
            if self.activate_disk_save == "y":
                self.save_chain_to_disk()

        elif t=="chain_request":
            if Chain.instance is None:
                return
            pkt={
                "type":"chain",
                "id":str(uuid.uuid4()),
                "chain":Chain.instance.to_block_dict_list()
            }
            await websocket.send(json.dumps(pkt))

        elif t=="chain":
            print("Received a Chain")
            try:
                block_list: List[Block]=[self.block_dict_to_block(d) for d in msg["chain"]]
            except Exception:
                print("\nMalformed Chain\n")
                return

            if not isvalidChain(block_list):
                print("\nInvalid Chain\n")
                return

            # If chain doesn't already exist we assign this as the chain
            if Chain.instance is None:
                self.chain=Chain(blockList=block_list)
                self.rebuild_contracts()
                print("\nInitialized Chain\n")
                if self.activate_disk_save == "y":
                    self.save_chain_to_disk()
                return

            # A chain that starts from a different genesis block belongs to a different network
            if block_list[0].hash != Chain.instance.chain[0].hash:
                print("\nChain has a different genesis block, ignoring\n")
                return

            if Chain.instance.rewrite(block_list):
                print("\nCurrent chain replaced by longer chain\n")
                self.rebuild_contracts()
                if self.miner:
                    self._abort_mining()
                if self.activate_disk_save == "y":
                    self.save_chain_to_disk()
            else:
                print("\nCurrent Chain Longer than received chain")

            async with self.mem_pool_condition:
                self.mem_pool[:]=[tx for tx in self.mem_pool if not Chain.instance.transaction_exists_in_chain(tx)]

            async with self.file_hashes_lock:
                for hash in list(self.file_hashes.keys()):
                    if(Chain.instance.cid_exists_in_chain(hash)):
                        self.file_hashes.pop(hash, None)

    def self_endpoint(self):
        """Our own (ip, port), resolved once, so "localhost" and "127.0.0.1" compare equal."""
        if getattr(self, "_self_endpoint", None) is None:
            try:
                self._self_endpoint = normalize_endpoint((self.host, self.port))
            except OSError:
                self._self_endpoint = (self.host, self.port)
        return self._self_endpoint

    def _abort_mining(self):
        """Stops the proof-of-work search (a cancelled asyncio task does NOT stop its worker thread)."""
        if self.mine_stop is not None:
            self.mine_stop.set()
        if self.mine_task and not self.mine_task.done():
            self.mine_task.cancel()

    def rebuild_contracts(self):
        """Re-creates the contract database from the chain (needed after adopting/replacing a chain)."""
        for block in Chain.instance.chain:
            for transaction in block.transactions:
                if transaction.receiver == "deploy":
                    self.deploy_contract(transaction)

    async def handle_connections(self, websocket):
        """
            We handle our server connections from here.
            Primary job is to simply read messages and send it to
            handle_messages function
        """
        peer_addr=(websocket.remote_address[0], websocket.remote_address[1])
        self.server_connections.add(websocket)

        print(f"Inbound Connection from {peer_addr[0]}:{peer_addr[1]}")

        try:
            async for raw in websocket:
                try:
                    msg=json.loads(raw)
                except ValueError:
                    continue
                if isinstance(msg, dict):
                    await self.handle_messages(websocket, msg)

        except websockets.exceptions.ConnectionClosed:
            print(f"Inbound Connection Closed: {peer_addr}")

        finally:
            self.server_connections.discard(websocket)
            self.got_pong.pop(websocket, None)
            self.have_sent_peer_info.pop(websocket, None)
            try:
                await websocket.close()
            except Exception:
                pass

    async def broadcast_message(self, pkt):
        # For broadcasting messages to all the connections we have
        if pkt is None:
            return
        data=json.dumps(pkt)

        for ws in list(self.server_connections | self.client_connections):
            try:
                await ws.send(data)
            except Exception as e:
                print(f"Error broadcasting: {e}")
                self.server_connections.discard(ws)
                self.client_connections.discard(ws)
                self.got_pong.pop(ws, None)
                self.have_sent_peer_info.pop(ws, None)
                try:
                    self.outbound_peers.discard(normalize_endpoint((ws.remote_address[0], ws.remote_address[1])))
                except Exception:
                    pass
                try:
                    await ws.close()
                except Exception:
                    pass

    async def create_and_broadcast_tx(self, receiver_public_key, payload):
        """
            Function to create and broadcast transactions
        """
        transaction=Transaction(payload, self.wallet.public_key_pem, receiver_public_key)
        transaction_str=str(transaction)
        
        signature=self.wallet.private_key.sign(transaction_str.encode())

        signature_b64=base64.b64encode(signature).decode()
        # b64encode returns bytes, Decode converts bytes to string
        
        pkt={
            "type":"new_tx",
            "id":str(uuid.uuid4()),
            "transaction":transaction_str,
            "sign":signature_b64,
            "sender_pem":self.wallet.public_key_pem # Already available as a pem string as defined in constructor
        }
        
        self.seen_message_ids.add(pkt["id"])
        if Chain.instance.transaction_exists_in_chain(transaction):
            return
        
        async with self.mem_pool_condition:
                self.mem_pool.append(transaction)

        transaction.sign=signature
        print("Transaction Created", transaction)
        print("\n")
        await self.broadcast_message(pkt)

    def get_contract_state(self, contract_id):
        for block in reversed(Chain.instance.chain):
            for transaction in reversed(block.transactions):
                if transaction.receiver == "invoke" and transaction.payload[0] == contract_id:
                    return transaction.payload[3]
        return {}

    async def user_input_handler(self):
        """
            A function to constantly take input from the user 
            about whom to send and how much
        """
        while True:
            print("Block Chain Menu\n***************")
            print("1) Add Transaction\n2) View balance\n3) Print Chain\n4) Print Pending Transactions\n5) Send Files\n6) Download Files\n7) Quit\n")

            ch= await asyncio.get_running_loop().run_in_executor(
                None, input, "Enter Your Choice: "
            )
            try:
                ch=int(ch)
            except:
                print("\nPlease enter a valid number!!!\n")
                continue
            if ch in (1, 2, 3) and Chain.instance is None:
                print("\nStill syncing the chain from the network, try again in a moment\n")
                continue
            if ch==1:
                rec = await asyncio.get_running_loop().run_in_executor(
                    None, input, "\nEnter Receiver's Name or Public Key: "
                )

                if rec == "deploy":
                    contract_code = get_contract_code_from_notepad()
                    if not contract_code:
                        print("Contract code field is empty")
                        continue
                    gas_used = len(contract_code)//10 + BASE_DEPLOY_COST
                    amount = gas_used * GAS_PRICE
                    payload = [contract_code, amount]

                    if amount<=Chain.instance.calc_balance(self.wallet.public_key_pem, self.mem_pool):
                        await self.create_and_broadcast_tx(rec, payload)
                    else:
                        print("Insufficient Account Balance")
                elif rec == "invoke":
                    contract_id = await asyncio.get_running_loop().run_in_executor(
                        None, input, "\nEnter Contract Id: "
                    )
                    if contract_id not in self.contractsDB.contracts:
                        print("No such contract found...")
                        continue

                    func_name = await asyncio.get_running_loop().run_in_executor(
                        None, input, "\nEnter Function Name: "
                    )

                    args = []
                    loop = asyncio.get_running_loop()
                    arg_number = 1
                    while True:
                        arg = await loop.run_in_executor(None, input, f"Enter argument {arg_number} (or \\q to finish): ")
                        if arg.strip() == "\\q":
                            break
                        try:
                            parsed_arg = ast.literal_eval(arg)
                        except Exception:
                            parsed_arg = arg
                        args.append(parsed_arg)
                        arg_number += 1

                    try:
                        response = await asyncio.to_thread(self.run_contract, [contract_id, func_name, args])
                    except Exception as e:
                        print("Error: ", e)
                        continue
                    if(response["error"] != None):
                        print("Error: ", response["error"])
                        continue
                    state = response["state"]
                    gas_used = response["gas_used"]
                    amount = gas_used * GAS_PRICE

                    payload = [contract_id, func_name, args, state, amount]

                    if amount<=Chain.instance.calc_balance(self.wallet.public_key_pem, self.mem_pool):
                        await self.create_and_broadcast_tx(rec, payload)
                    else:
                        print("Insufficient Account Balance")
                else:
                    amt= await asyncio.get_running_loop().run_in_executor(
                        None, input, "\nEnter Amount to send: "
                    )

                    receiver_public_key = self.name_to_public_key_dict.get(rec.lower().strip())

                    if receiver_public_key is None:
                        rec_split = rec.split("\\n")
                        rec_refined = "\n".join(rec_split)
                        exist = 0
                        for (nme, pk) in self.name_to_public_key_dict.items():
                            if pk == rec_refined:
                                receiver_public_key = pk
                                exist = 1
                                break
                        if exist == 0:
                            print("No person available in directory with provided name or public key...")
                            continue
                    
                    try:
                        amt=float(amt)
                    except ValueError:
                        print("Amount must be a number")
                        continue

                    if not math.isfinite(amt) or amt<=0:
                        print("\nAmount must be positive\n")
                        continue

                    if amt<=Chain.instance.calc_balance(self.wallet.public_key_pem, self.mem_pool):
                        await self.create_and_broadcast_tx(receiver_public_key, amt)
                    else:
                        print("Insufficient Account Balance")
            elif ch==2:
                print("Account Balance =",Chain.instance.calc_balance(self.wallet.public_key_pem, self.mem_pool))
            elif ch==3:
                i=0
                # We print all the blocks
                for block in Chain.instance.chain:
                    print(f"block{i}: {block}\n")
                    i+=1            
            elif ch==4:
                i=0
                for transaction in self.mem_pool:
                    print(f"transaction{i}: {transaction}\n\n")
                    i+=1

            elif ch==5:
                desc= await asyncio.get_running_loop().run_in_executor(
                    None, input, "\nEnter description of file: "
                )
                path= await asyncio.get_running_loop().run_in_executor(
                    None, input, "\nEnter path of file: "
                )
                pkt=await self.uploadFile(desc, path)
                if pkt:
                    await self.broadcast_message(pkt)

            elif ch==6:
                cid= await asyncio.get_running_loop().run_in_executor(
                    None, input, "\nEnter cid of file: "
                )
                path= await asyncio.get_running_loop().run_in_executor(
                    None, input, "\nEnter path to download the file: "
                )
                await asyncio.to_thread(download_ipfs_file_subprocess, cid, path)

            elif ch==7:
                print("Quitting...")
                break

    async def uploadFile(self, desc: str, path:str):
        if not shutil.which("ipfs"):
            print("\nIPFS is not installed, cannot upload files\n")
            return None
        file_path=Path(path)
        if(not file_path.is_file()):
            print("\nFile doesn't exist\n")
            return

        if not self.daemon_process: 
            self.start_daemon()
        
        cid, name = await asyncio.to_thread(addToIpfs, path)
        if(not(cid and name)):
            return
        
        print(f"\nNew File Created : {cid}\n")
        pkt={
            "type":"file",
            "id":str(uuid.uuid4()),
            "desc":desc,
            "cid":cid
        }
        
        self.seen_message_ids.add(pkt["id"])
        async with self.file_hashes_lock:
            self.file_hashes[cid]=desc
        return pkt

    def init_repo(self):
        """
            Creates a ipfs repo of name ending in ipfs_port_no eg ipfs_5000 
        """
        if not self.repo_path.exists():
            subprocess.run(["ipfs", "init"], env=self.env, check=True)
            print("\nIPFS repo created\n")

    def configure_ports(self):
        subprocess.run(["ipfs", "config", "Addresses.API", f"/ip4/127.0.0.1/tcp/{self.ipfs_port}"], env=self.env, check=True)
        subprocess.run(["ipfs", "config", "Addresses.Gateway", f"/ip4/127.0.0.1/tcp/{self.gateway_port}"], env=self.env, check=True)
        subprocess.run([
            "ipfs", "config", "Addresses.Swarm", "--json",
            f'["/ip4/127.0.0.1/tcp/{self.swarm_tcp}", "/ip4/127.0.0.1/udp/{self.swarm_udp}/quic"]'
        ], env=self.env, check=True)
        print("\nConfigured Ports\n")

    def start_daemon(self):
        self.daemon_process= subprocess.Popen(["ipfs", "daemon"], env=self.env)
        print("\nIPFS Daemon Started\n")

    def stop_daemon(self):
        if self.daemon_process:
            self.daemon_process.terminate()
            self.daemon_process.wait()

    async def connect_to_peer(self, host, port):
        """
            Function to form an outbound connection to the given host:port
            and handle messages that come form this connection
            Also initiates the handshake
        """

        endpoint=(host, port)
        if endpoint in self.outbound_peers or endpoint==self.self_endpoint():
            return
        self.outbound_peers.add(endpoint)   # claim it now so two tasks never dial the same peer

        uri=f"ws://{host}:{port}"

        websocket = None
        try:
            websocket=await websockets.connect(uri)
            self.client_connections.add(websocket)
            self.have_sent_peer_info[websocket]=False

            print(f"Outbound connection formed to {host}:{port}")

            pkt = None
            # If connecting first time to the network, broadcasts node information to the entire network
            if Chain.instance is None:
                pkt={
                    "type":"add_peer",
                    "id":str(uuid.uuid4()),
                    "data":{
                        "host":self.host,
                        "port":self.port,
                        "name":self.name,
                        "public_key":self.wallet.public_key_pem
                    }
                }
            else:
                pkt={
                    "type":"ping",
                    "id":str(uuid.uuid4()),
                }

            self.seen_message_ids.add(pkt["id"])
            await websocket.send(json.dumps(pkt))

            async for raw in websocket:
                try:
                    msg=json.loads(raw)
                except ValueError:
                    continue
                if isinstance(msg, dict):
                    await self.handle_messages(websocket, msg)
        except Exception as e:
            print(f"Failed to connect to {host}:{port} ::: {e}")
        finally:
            self.outbound_peers.discard(endpoint)
            if websocket is not None:   # it is None when the connection attempt itself failed
                self.client_connections.discard(websocket)
                self.got_pong.pop(websocket, None)
                self.have_sent_peer_info.pop(websocket, None)
                try:
                    await websocket.close()
                except Exception:
                    pass

    async def discover_peers(self):
        """
            Maintains up to MAX_CONNECTIONS peers.
            Connects only to fill the pool if under MAX_CONNECTIONS.
        """

        while True:
            if len(self.outbound_peers) < MAX_CONNECTIONS:
                potential_peers = {
                    endpoint for endpoint in self.known_peers
                    if endpoint not in self.outbound_peers and endpoint != self.self_endpoint()
                }
                while len(self.outbound_peers) < MAX_CONNECTIONS and potential_peers:
                    new_peer = get_random_element(potential_peers)
                    potential_peers.discard(new_peer)
                    if new_peer:
                        asyncio.create_task(self.connect_to_peer(*new_peer))
                        await asyncio.sleep(1)
            await asyncio.sleep(30)

    async def gossip_peer_sampler(self):
        """
            Every 60s, drops one existing peer and connects to one new peer.
        """
        while True:
            await asyncio.sleep(60)
            if len(self.known_peers) <= len(self.outbound_peers) or len(self.outbound_peers) < MAX_CONNECTIONS:
                continue  # Nothing to swap

            # Disconnect one random client connection
            to_drop = get_random_element(self.client_connections)
            if to_drop:
                print(f"Gossip Sampling: Disconnecting {to_drop.remote_address}")
                self.client_connections.discard(to_drop)
                normalized_endpoint = normalize_endpoint((to_drop.remote_address[0], to_drop.remote_address[1]))
                self.outbound_peers.discard(normalized_endpoint)
                self.got_pong.pop(to_drop, None)
                self.have_sent_peer_info.pop(to_drop, None)
                await to_drop.close()
                await to_drop.wait_closed()

            # Connect to a new peer (not already connected)
            potential_peers = {
                endpoint for endpoint in self.known_peers
                if endpoint not in self.outbound_peers and endpoint != self.self_endpoint()
            }

            if potential_peers:
                new_peer = get_random_element(potential_peers)
                if new_peer:
                    print(f"Gossip Sampling: Connecting to new peer {new_peer}")
                    asyncio.create_task(self.connect_to_peer(*new_peer))

    async def mine_blocks(self):
        """
            Every 30 seconds, bundle the valid pending transactions into a block and mine it.
        """
        while True:
            await asyncio.sleep(30)
            if Chain.instance is None:
                continue

            async with self.mem_pool_condition:
                # Drop transactions that are already on chain or that can no longer be paid for,
                # otherwise one bad transaction would make every block we build invalid.
                candidates=[]
                for transaction in self.mem_pool:
                    amount=get_tx_amount(transaction)
                    if amount is None or amount<=0 or Chain.instance.transaction_exists_in_chain(transaction):
                        continue
                    if amount>Chain.instance.calc_balance(transaction.sender, candidates):
                        continue
                    candidates.append(transaction)
                self.mem_pool[:]=candidates
                transaction_list=list(candidates)

            if not transaction_list:
                continue

            newBlock=Block(Chain.instance.lastBlock.hash, transaction_list)
            newBlock.files=self.file_hashes.copy()
            newBlock.miner=self.wallet.public_key_pem   # set BEFORE mining: it is part of the hash

            stop_event=threading.Event()
            self.mine_stop=stop_event
            nonce=await asyncio.to_thread(Chain.instance.mine, newBlock, stop_event)
            if nonce is None:
                print("\nMining stopped: someone else found a block first\n")
                continue

            if not Chain.instance.isValidBlock(newBlock):
                print("\n Invalid Block \n")
                continue

            Chain.instance.chain.append(newBlock)
            print("\nBlock Appended \n")

            for transaction in newBlock.transactions:
                if transaction.receiver == "deploy":
                    self.deploy_contract(transaction)

            async with self.file_hashes_lock:
                for hash in list(self.file_hashes.keys()):
                    if newBlock.cid_exists_in_block(hash):
                        self.file_hashes.pop(hash, None)

            async with self.mem_pool_condition:
                self.mem_pool[:]=[tx for tx in self.mem_pool if not newBlock.transaction_exists_in_block(tx)]

            pkt={
                "type":"new_block",
                "id":str(uuid.uuid4()),
                "block":newBlock.to_dict(),
                "miner":self.wallet.public_key_pem
            }
            self.seen_message_ids.add(pkt["id"])
            await self.broadcast_message(pkt)
            if self.activate_disk_save == "y":
                self.save_chain_to_disk()

    def calculate_contract_id(self, sender, timestamp):
        data = f"{sender}:{timestamp}"
        hash_object = hashlib.sha256(data.encode('utf-8'))
        return hash_object.hexdigest()
        
    def deploy_contract(self, transaction: Transaction):
        sender = transaction.sender
        timestamp = transaction.ts
        code = transaction.payload[0]
        contract_id = self.calculate_contract_id(sender, timestamp)
        self.contractsDB.store_contract(contract_id, code)
        print("Contract deployed with id: ", contract_id)

    def run_contract(self, payload):
        contract_id, func_name, args = payload[0], payload[1], payload[2]
        code = self.contractsDB.get_contract(contract_id)
        if code is None:
            raise Exception(f"Contract '{contract_id}' not found.")

        state = self.get_contract_state(contract_id)
        executor = SecureContractExecutor(code)
        response = executor.run(func_name, args, state)

        return response

    async def find_longest_chain(self):
        """
            We routinely check every 30 seconds, every other chain and we replace
            ours with theirs if theirs is >= ours
        """
        while True:
            pkt={
                "type":"chain_request",
                "id":str(uuid.uuid4())
            }
            self.seen_message_ids.add(pkt["id"])
            await self.broadcast_message(pkt)
            print("\nSent out chain requests...")
            await asyncio.sleep(60)

    async def start(self, bootstrap_host=None, bootstrap_port=None):
        # We start the server
        server = await websockets.serve(self.handle_connections, self.host, self.port)

        if bootstrap_host and bootstrap_port:
            # If bootstrap node is given we connect to it and take its chain
            normalized_bootstrap_host, normalized_bootstrap_port = normalize_endpoint((bootstrap_host, bootstrap_port))
            asyncio.create_task(self.connect_to_peer(normalized_bootstrap_host, normalized_bootstrap_port))
        elif Chain.instance is None:
            self.chain=Chain(publicKey=self.wallet.public_key_pem)
        else:
            # A chain was restored from disk. Calling Chain(...) again used to hand back an
            # uninitialised object here.
            self.chain=Chain.instance

        if Chain.instance is not None:
            self.rebuild_contracts()

        inp_task=asyncio.create_task(self.user_input_handler())
        consensus_task=asyncio.create_task(self.find_longest_chain())
        disc_task=asyncio.create_task(self.discover_peers())
        sampler_task = asyncio.create_task(self.gossip_peer_sampler())

        if self.miner:
            self.mine_task=asyncio.create_task(self.mine_blocks())

        # IPFS is only needed for file sharing. Without it installed, the old code crashed at startup.
        if shutil.which("ipfs"):
            self.init_repo()
            self.configure_ports()
        else:
            print("\n'ipfs' not found on PATH: file upload/download disabled, everything else works.\n")

        try:
            await inp_task
        finally:
            disc_task.cancel()
            consensus_task.cancel()
            sampler_task.cancel()
            if self.miner:
                self._abort_mining()
            if self.daemon_process:
                self.stop_daemon()
            server.close()
            await server.wait_closed()

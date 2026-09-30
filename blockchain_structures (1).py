import json, hashlib, math
from typing import List, Optional
from ecdsa import VerifyingKey
from shared_blockchain_structures import (
    Transaction,
    BaseBlock,
    CommonChain,
    Wallet,
    txs_to_json_digestable_form,
    valid_chain_length,
)

__all__ = ["Transaction", "Block", "Chain", "Wallet", "isvalidChain",
           "get_tx_amount", "calc_balance_block_list", "transaction_exists_in_block_list"]

POW_PREFIX = "00000"   # Proof-of-work difficulty: hash must start with this
MINER_REWARD = 6


def get_tx_amount(transaction):
    """
        Returns the amount a transaction moves, or None if it is malformed.
        Contract transactions ("deploy"/"invoke") keep their fee as the last payload item.
        NaN / infinity / bool / non-numbers are rejected: NaN in particular slips past
        every ">" and "<=" comparison, so it must never be treated as a valid amount.
    """
    try:
        if transaction.receiver in ("deploy", "invoke"):
            amount = transaction.payload[-1]
        else:
            amount = transaction.payload
        if isinstance(amount, bool) or not isinstance(amount, (int, float)):
            return None
        if not math.isfinite(amount):
            return None
        return amount
    except Exception:
        return None


def _balance(blocks, publicKey, count, pending_transactions=None):
    """
        Balance of publicKey using the first `count` blocks, minus what the
        (not yet confirmed) pending transactions would spend. We do not credit
        pending receipts because they could still turn out to be invalid.
    """
    bal = 0
    for block in blocks[:count]:
        for transaction in block.transactions:
            amount = get_tx_amount(transaction) or 0
            if transaction.sender == publicKey:
                bal -= amount
            if transaction.receiver == publicKey:   # 'if', not 'elif': sending to yourself nets to zero
                bal += amount
        if block.miner == publicKey:
            bal += MINER_REWARD

    for transaction in pending_transactions or []:
        if transaction.sender == publicKey:
            bal -= get_tx_amount(transaction) or 0
    return bal


class Block(BaseBlock):
    def __init__(self, prevHash: str, transactions: List[Transaction], ts=None, nonce=None, id=None):
        super().__init__(prevHash, transactions, ts, id)
        self.nonce = nonce or 0
        # The miner is part of the hashed data. Otherwise any node relaying the block
        # could swap the miner field and steal the block reward.
        self.miner: Optional[str] = None

    def to_dict(self):
        return {
            "id": self.id,
            "prevHash": self.prevHash,
            "transactions": txs_to_json_digestable_form(self.transactions),
            "ts": self.ts,
            "nonce": self.nonce,
            "files": self.files,
            "miner": self.miner,
        }

    def __str__(self):
        return json.dumps(self.to_dict())

    @property  # Now you can access hash like this myblock.hash
    def hash(self):
        block_str = json.dumps(self.to_dict())
        return hashlib.sha256(block_str.encode()).hexdigest()


class Chain(CommonChain):
    instance = None  # Class variable

    def __init__(self, publicKey: str = None, blockList: List[Block] = None):
        """
            If we are the first node, we mine the genesis block for ourself
            otherwise we receive blockList from the network and assign that to be the chain
        """
        if Chain.instance is not None:
            # The old code silently returned an half-built object here
            raise RuntimeError("A Chain already exists, use Chain.instance")

        Chain.instance = self
        try:
            if publicKey and not blockList:
                genesis_block = Block(None, [Transaction(50, "Genesis", publicKey)])
                super().__init__(genesis_block=genesis_block)
                self.mine(self.chain[0])
            elif blockList and not publicKey:
                super().__init__(block_list=blockList)
            else:
                raise ValueError("Invalid arguments")
        except Exception:
            Chain.instance = None
            raise

    def mine(self, block: Block, stop_event=None):
        """
            Finds a nonce. If stop_event (threading.Event) is set while we are searching
            (because someone else already found the next block) we give up and return None.
        """
        block.nonce = 0
        print("Mining...")

        while not block.hash.startswith(POW_PREFIX):
            block.nonce += 1
            if stop_event is not None and block.nonce % 500 == 0 and stop_event.is_set():
                return None

        print(f"Solution Found!!! nonce = {block.nonce} hash = {block.hash}")
        return block.nonce

    def rewrite(self, blockList: List[Block]):
        if len(self.chain) >= len(blockList):
            return False
        self.chain = blockList.copy()
        return True

    def isValidBlock(self, block: Block):
        # Verify PoW
        if not block.hash.startswith(POW_PREFIX):
            print(f"Problem with pow hash = {block.hash} nonce={block.nonce}")
            return False

        if self.lastBlock.hash != block.prevHash:
            print("Hash Problem")
            print(f"Actual prev hash: {self.lastBlock.hash}\nBlock's prev hash: {block.prevHash}")
            return False

        if not isinstance(block.miner, str) or not block.miner:
            print("Block has no miner")
            return False

        if not block.transactions:
            print("Empty block")
            return False

        mem_pool: List[Transaction] = []
        seen_ids = set()
        for transaction in block.transactions:
            if transaction.id in seen_ids or self.transaction_exists_in_chain(transaction):
                print("Duplicate transaction(s)")
                return False
            seen_ids.add(transaction.id)

            try:
                vk_tx = VerifyingKey.from_pem(transaction.sender.encode())
                vk_tx.verify(transaction.sign, str(transaction).encode())
            except Exception:
                print("\nInvalid signature on transaction\n")
                return False

            amount = get_tx_amount(transaction)
            if amount is None or amount <= 0:
                print("Invalid amount")
                return False

            # The transactions already accepted in this same block count as spent
            if amount > self.calc_balance(publicKey=transaction.sender, pending_transactions=mem_pool):
                print("Insufficient balance")
                return False
            mem_pool.append(transaction)

        return True

    def calc_balance(self, publicKey, pending_transactions: List[Transaction] = None):
        return _balance(self.chain, publicKey, valid_chain_length(len(self.chain)), pending_transactions)


def calc_balance_block_list(block_list: List[Block], publicKey, i, pending_transactions: List[Transaction] = None):
    return _balance(block_list, publicKey, valid_chain_length(i), pending_transactions)


def transaction_exists_in_block_list(blockList: List[Block], transaction_tc: Transaction, idx):
    """True if the transaction already appears in one of the first idx blocks."""
    for i in range(idx):
        for transaction in blockList[i].transactions:
            if transaction.id == transaction_tc.id:
                return True
    return False


def isvalidChain(blockList: List[Block]):
    if not blockList:
        return False

    for i in range(1, len(blockList)):   # block 0 is the genesis block
        currBlock = blockList[i]

        if not currBlock.hash.startswith(POW_PREFIX):
            print("\nNo POW\n")
            return False

        if currBlock.prevHash != blockList[i - 1].hash:
            print("\nPrev hash is incorrect\n")
            return False

        if not isinstance(currBlock.miner, str) or not currBlock.miner or not currBlock.transactions:
            print("\nBlock has no miner or no transactions\n")
            return False

        mem_pool = []
        seen_ids = set()
        for transaction in currBlock.transactions:
            if transaction.id in seen_ids or transaction_exists_in_block_list(blockList, transaction, i):
                print("Duplicate transaction(s)")
                return False
            seen_ids.add(transaction.id)

            try:
                vk_tx = VerifyingKey.from_pem(transaction.sender.encode())
                vk_tx.verify(transaction.sign, str(transaction).encode())
            except Exception:
                print("\nInvalid signature on transaction\n")
                return False

            amount = get_tx_amount(transaction)
            if amount is None or amount <= 0:
                return False
            if calc_balance_block_list(blockList, transaction.sender, i, mem_pool) < amount:
                return False
            mem_pool.append(transaction)

    print("Chain OK: valid PoW, no duplicate transactions, no invalid signatures, no invalid amounts\n")
    return True

import logging
from typing import Dict, Optional, Any

# Configure logging for tracking database operations
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SmartContractDatabase")

class SmartContractDatabase:
    """In-memory database for storing and managing smart contract code."""

    def __init__(self) -> None:
        self._contracts: Dict[str, str] = {}

    def store_contract(self, contract_id: str, code: str) -> None:
        """Stores or updates a smart contract in the database.
        
        Args:
            contract_id: Unique string identifier for the contract.
            code: Source code of the smart contract.
            
        Raises:
            ValueError: If contract_id or code are empty or not strings.
        """
        if not isinstance(contract_id, str) or not contract_id.strip():
            raise ValueError("contract_id must be a non-empty string.")
        if not isinstance(code, str) or not code.strip():
            raise ValueError("code must be a non-empty string.")

        self._contracts[contract_id.strip()] = code
        logger.info(f"Contract '{contract_id}' successfully stored.")

    def get_contract(self, contract_id: str) -> Optional[str]:
        """Retrieves a smart contract by its identifier.
        
        Args:
            contract_id: Unique string identifier for the contract.
            
        Returns:
            The code string if found, or None if not found.
            
        Raises:
            ValueError: If contract_id is empty or not a string.
        """
        if not isinstance(contract_id, str) or not contract_id.strip():
            raise ValueError("contract_id must be a non-empty string.")

        code = self._contracts.get(contract_id.strip())
        if code is None:
            logger.warning(f"Contract '{contract_id}' not found.")
        return code

    def delete_contract(self, contract_id: str) -> bool:
        """Deletes a contract from the database.
        
        Returns:
            True if deleted, False if the contract was not found.
        """
        if not isinstance(contract_id, str) or not contract_id.strip():
            raise ValueError("contract_id must be a non-empty string.")
            
        cid = contract_id.strip()
        if cid in self._contracts:
            del self._contracts[cid]
            logger.info(f"Contract '{cid}' deleted.")
            return True
        return False

    def list_contracts(self) -> Dict[str, str]:
        """Returns a copy of all stored contracts."""
        return self._contracts.copy()

    def __len__(self) -> int:
        """Returns total count of stored contracts."""
        return len(self._contracts)
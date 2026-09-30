from smart_contract.smart_contract import ContractEnvironment

def sandbox_contract_runner(code, func_name, args, state, return_dict):
    try:
        env = ContractEnvironment(code)
        # Use a new variable for the updated state to avoid overwriting the original reference prematurely
        new_state, msg, gas_used = env.run_contract(func_name, args, state)
        return_dict['state'] = new_state
        return_dict['msg'] = msg
        return_dict['gas_used'] = gas_used
        return_dict['error'] = None
    except Exception as e:
        # Revert to the original state instead of None to prevent wiping contract data
        return_dict['state'] = state
        return_dict['msg'] = None
        return_dict['gas_used'] = 0
        return_dict['error'] = str(e)
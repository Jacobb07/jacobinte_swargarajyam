import multiprocessing
import time
import psutil
from smart_contract.sandbox_runner import sandbox_contract_runner

TIMEOUT = 20.0
MEMORY_LIMIT_MB = 500


def _failure(error):
    return {"success": False, "error": error, "state": None, "msg": None, "gas_used": 0}


class SecureContractExecutor:
    def __init__(self, code: str):
        self.code = code

    def run(self, func_name: str, args, state):
        # 'with' shuts the Manager's helper process down. The old code created a new
        # Manager on every call and never closed it, leaking one process per contract run.
        with multiprocessing.Manager() as manager:
            return_dict = manager.dict()

            process = multiprocessing.Process(
                target=sandbox_contract_runner,
                args=(self.code, func_name, args, state, return_dict),
                daemon=True,
            )
            process.start()

            try:
                proc = psutil.Process(process.pid)
            except psutil.NoSuchProcess:   # it already finished
                proc = None

            start_time = time.monotonic()
            failure = None

            while process.is_alive():
                if time.monotonic() - start_time > TIMEOUT:
                    failure = "Execution timeout"
                    break

                if proc is not None:
                    try:
                        mem_usage_mb = proc.memory_info().rss / (1024 * 1024)
                        if mem_usage_mb > MEMORY_LIMIT_MB:
                            failure = f"Memory limit exceeded ({int(mem_usage_mb)} MB)"
                            break
                    except psutil.NoSuchProcess:
                        break

                process.join(0.05)

            if failure:
                process.terminate()
                process.join(2)
                if process.is_alive():
                    process.kill()
                    process.join()
                return _failure(failure)

            process.join()
            result = dict(return_dict)

        error = result.get("error")
        if error is None and process.exitcode not in (0, None):
            error = f"Contract process crashed (exit code {process.exitcode})"
        if error is None and "state" not in result:
            error = "Contract produced no result"

        return {
            "success": error is None,
            "error": error,
            "state": result.get("state"),
            "msg": result.get("msg"),
            "gas_used": result.get("gas_used", 0),
        }

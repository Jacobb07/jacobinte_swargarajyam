import sys

GAS_LIMIT = 10000

class GasMeter:
    def __init__(self, limit=GAS_LIMIT):
        self.gas_limit = limit
        self.gas_used = 0

    def tracer(self, frame, event, arg):
        if event == "line":
            self.gas_used += 1
            if self.gas_used > self.gas_limit:
                self.stop()
                raise Exception("Out of gas")
        return self.tracer

    def start(self):
        self.gas_used = 0
        sys.settrace(self.tracer)

    def stop(self):
        sys.settrace(None)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False
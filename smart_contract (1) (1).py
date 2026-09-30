import math
import operator

from RestrictedPython import compile_restricted
from RestrictedPython.Eval import default_guarded_getiter, default_guarded_getitem
from RestrictedPython.Guards import (
    safe_builtins,
    safer_getattr,
    full_write_guard,
    guarded_iter_unpack_sequence,
    guarded_unpack_sequence,
)
from smart_contract.gas_meter import GasMeter

_INPLACE_OPS = {
    '+=': operator.iadd, '-=': operator.isub, '*=': operator.imul,
    '/=': operator.itruediv, '//=': operator.ifloordiv, '%=': operator.imod,
    '**=': operator.ipow, '<<=': operator.ilshift, '>>=': operator.irshift,
    '&=': operator.iand, '^=': operator.ixor, '|=': operator.ior,
}


def _inplacevar_(op, x, y):
    """Needed by RestrictedPython for every 'x += 1' style statement."""
    return _INPLACE_OPS[op](x, y)


class ContractEnvironment:
    def __init__(self, code: str):
        self.code = code

        extended_builtins = dict(safe_builtins)
        extended_builtins.update({
            'set': set,
            'dict': dict,
            'list': list,
            'len': len,
            'range': range,
            'min': min,
            'max': max,
            'sum': sum,
            'abs': abs,
            'sorted': sorted,
            'enumerate': enumerate,
            'zip': zip,
            'any': any,
            'all': all,

            # math functions
            'sqrt': math.sqrt,
            'ceil': math.ceil,
            'floor': math.floor,
            'pow': pow,
            'fabs': math.fabs,
            'log': math.log,
            'log10': math.log10,
            'exp': math.exp,
            'sin': math.sin,
            'cos': math.cos,
            'tan': math.tan,
            'degrees': math.degrees,
            'radians': math.radians,
            'pi': math.pi,
            'e': math.e,
            'isclose': math.isclose,
            'gcd': math.gcd,
            'factorial': math.factorial,
        })

        # Every "_xxx_" hook below is something RestrictedPython's compiled code calls.
        # Without _getattr_ / _inplacevar_ / unpack hooks, ordinary contract code such as
        # `state.get(...)`, `total += 1` or `a, b = pair` fails with a NameError.
        self.globals = {
            '__builtins__': extended_builtins,
            '__name__': 'contract',
            '_getiter_': default_guarded_getiter,
            '_getitem_': default_guarded_getitem,
            '_getattr_': safer_getattr,
            '_write_': full_write_guard,
            '_inplacevar_': _inplacevar_,
            '_iter_unpack_sequence_': guarded_iter_unpack_sequence,
            '_unpack_sequence_': guarded_unpack_sequence,
        }
        self._compile()

    def _compile(self):
        self.compiled = compile_restricted(self.code, filename='<contract>', mode='exec')
        # ONE namespace for globals and locals. With two separate dicts, functions defined by
        # the contract land in `locals`, so a contract function could not call another
        # contract function (helper / recursion) and raised NameError.
        exec(self.compiled, self.globals)

    def run_contract(self, func_name: str, args, state):
        func = self.globals.get(func_name)
        if not callable(func) or func_name.startswith('_'):
            raise Exception(f"Function '{func_name}' not found in contract.")
        if not isinstance(args, (list, tuple)):
            raise Exception("Contract arguments must be a list.")

        gas_meter = GasMeter()

        try:
            gas_meter.start()
            state, msg = func(*args, state)
        finally:
            gas_meter.stop()

        return state, msg, gas_meter.gas_used

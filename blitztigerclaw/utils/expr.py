import ast
import operator
from functools import lru_cache

from blitztigerclaw.exceptions import ExpressionError

# Try native C engine first — 20-50x faster for filter/compute
try:
    from blitztigerclaw.native.expr_engine import (
        compile_expr as _native_compile,
        eval_filter as native_eval_filter,
        eval_compute as native_eval_compute,
        native_select,
        native_dedupe,
        native_sort,
    )
    NATIVE_AVAILABLE = True
except ImportError:
    NATIVE_AVAILABLE = False
    native_eval_filter = None
    native_eval_compute = None
    native_select = None
    native_dedupe = None
    native_sort = None

SAFE_OPS = {
    ast.Gt: operator.gt,
    ast.Lt: operator.lt,
    ast.GtE: operator.ge,
    ast.LtE: operator.le,
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Mod: operator.mod,
    ast.FloorDiv: operator.floordiv,
}

BLOCKED_NAMES = frozenset({
    "exec", "eval", "compile", "__import__", "open",
    "input", "globals", "locals", "vars", "dir",
    "getattr", "setattr", "delattr", "breakpoint",
})

SAFE_BUILTINS = frozenset({
    "len", "int", "float", "str", "bool", "abs",
    "min", "max", "sum", "round", "sorted", "list",
    "upper", "lower", "strip", "replace", "split",
    "startswith", "endswith", "title",
})

def _can_use_native(expr_str: str) -> bool:
    """Check if expression is simple enough for the native C engine."""
    if not NATIVE_AVAILABLE:
        return False
    # Native engine handles: fields, comparisons, arithmetic, and/or/not, constants
    # Does NOT handle: method calls (.upper()), ternary (if/else), function calls
    import re
    # Check for method calls: letter.letter pattern (not number.number)
    if re.search(r'[a-zA-Z_]\.(?![0-9])', expr_str):
        return False
    # Check for function calls (but allow parenthesized subexpressions)
    if re.search(r'[a-zA-Z_]\s*\(', expr_str):
        return False
    # Check for ternary
    if ' if ' in expr_str or ' else ' in expr_str:
        return False
    return True


def compile_expr(expr_str: str):
    """Compile a filter/compute expression into a safe callable.

    Uses native C engine when available (20-50x faster).
    Falls back to Python AST evaluator for complex expressions.
    """
    if _can_use_native(expr_str):
        try:
            return _native_compile(expr_str)
        except (ValueError, TypeError):
            pass  # Fall through to Python evaluator

    return _compile_python(expr_str)


@lru_cache(maxsize=256)
def _parse_and_validate(expr_str: str) -> ast.Expression:
    """Parse and validate an expression string. Cached for repeated calls."""
    try:
        tree = ast.parse(expr_str, mode="eval")
    except SyntaxError as e:
        raise ExpressionError(f"Invalid expression: {expr_str!r} — {e}")
    _validate_ast(tree)
    return tree


def _compile_python(expr_str: str):
    """Python AST-walking evaluator (fallback for complex expressions)."""
    tree = _parse_and_validate(expr_str)

    def evaluator(row: dict):
        try:
            return _eval_node(tree.body, row)
        except Exception:
            return None

    return evaluator


def _validate_ast(tree: ast.Expression):
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            raise ExpressionError("Imports not allowed in expressions")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in BLOCKED_NAMES:
                raise ExpressionError(
                    f"Function '{node.func.id}' not allowed in expressions"
                )


def _eval_node(node, row):
    if isinstance(node, ast.Compare):
        left = _eval_node(node.left, row)
        for op, comparator in zip(node.ops, node.comparators):
            right = _eval_node(comparator, row)
            if left is None or right is None:
                return False
            if not SAFE_OPS[type(op)](left, right):
                return False
        return True

    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            return all(_eval_node(v, row) for v in node.values)
        if isinstance(node.op, ast.Or):
            return any(_eval_node(v, row) for v in node.values)

    if isinstance(node, ast.BinOp):
        left = _eval_node(node.left, row)
        right = _eval_node(node.right, row)
        return SAFE_OPS[type(node.op)](left, right)

    if isinstance(node, ast.UnaryOp):
        operand = _eval_node(node.operand, row)
        if isinstance(node.op, ast.Not):
            return not operand
        if isinstance(node.op, ast.USub):
            return -operand

    if isinstance(node, ast.Name):
        return row.get(node.id)

    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.Attribute):
        obj = _eval_node(node.value, row)
        if obj is None:
            return None
        attr = node.attr
        if attr not in SAFE_BUILTINS:
            raise ExpressionError(f"Attribute '{attr}' not allowed")
        return getattr(obj, attr)

    if isinstance(node, ast.Call):
        func = _eval_node(node.func, row)
        if not callable(func):
            return None
        args = [_eval_node(a, row) for a in node.args]
        return func(*args)

    if isinstance(node, ast.IfExp):
        test = _eval_node(node.test, row)
        return _eval_node(node.body, row) if test else _eval_node(node.orelse, row)

    raise ExpressionError(f"Unsupported expression: {type(node).__name__}")

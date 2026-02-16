/*
 * expr_engine.c — Native C expression evaluator for Blitz v0.2.0
 *
 * Replaces Python hot paths with compiled C:
 * - Expression evaluation (filter, compute)
 * - Field selection (select)
 * - Deduplication (dedupe)
 * - Sorting (sort)
 *
 * Build: python setup_native.py build_ext --inplace
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

/* ---- Op codes for compiled expressions ---- */

typedef enum {
    OP_CONST_INT,
    OP_CONST_FLOAT,
    OP_CONST_STR,
    OP_CONST_BOOL,
    OP_CONST_NONE,
    OP_FIELD,
    OP_GT, OP_LT, OP_GE, OP_LE, OP_EQ, OP_NE,
    OP_AND, OP_OR, OP_NOT,
    OP_ADD, OP_SUB, OP_MUL, OP_DIV, OP_MOD,
    OP_END
} OpCode;

typedef struct {
    OpCode op;
    union {
        long long i_val;
        double f_val;
        char *s_val;
    };
} Instr;

typedef struct {
    Instr *code;
    int len;
    int cap;
} CompiledExpr;

#define MAX_STACK 64

typedef struct {
    PyObject *items[MAX_STACK];
    int top;
} EvalStack;

static inline void stack_push(EvalStack *s, PyObject *obj) {
    if (s->top < MAX_STACK) {
        Py_INCREF(obj);
        s->items[s->top++] = obj;
    }
}

static inline PyObject *stack_pop(EvalStack *s) {
    if (s->top > 0) return s->items[--s->top];
    Py_RETURN_NONE;
}

static inline void stack_clear(EvalStack *s) {
    while (s->top > 0) {
        Py_DECREF(s->items[--s->top]);
    }
}

/* ---- Tokenizer ---- */

typedef enum {
    TOK_INT, TOK_FLOAT, TOK_STR, TOK_NAME, TOK_BOOL, TOK_NONE,
    TOK_GT, TOK_LT, TOK_GE, TOK_LE, TOK_EQ, TOK_NE,
    TOK_AND, TOK_OR, TOK_NOT,
    TOK_PLUS, TOK_MINUS, TOK_STAR, TOK_SLASH, TOK_PERCENT,
    TOK_LPAREN, TOK_RPAREN,
    TOK_EOF, TOK_ERROR
} TokType;

typedef struct {
    TokType type;
    const char *start;
    int len;
    double f_val;
    long long i_val;
} Token;

typedef struct {
    const char *src;
    int pos;
    int len;
} Lexer;

static inline void skip_ws(Lexer *l) {
    while (l->pos < l->len && isspace(l->src[l->pos])) l->pos++;
}

static Token next_token(Lexer *l) {
    Token tok = {TOK_EOF, NULL, 0, 0, 0};
    skip_ws(l);
    if (l->pos >= l->len) return tok;

    const char *s = l->src;
    int p = l->pos;
    char c = s[p];

    if (c == '\'' || c == '"') {
        char quote = c;
        p++;
        int start = p;
        while (p < l->len && s[p] != quote) p++;
        tok.type = TOK_STR;
        tok.start = s + start;
        tok.len = p - start;
        if (p < l->len) p++;
        l->pos = p;
        return tok;
    }

    if (isdigit(c) || (c == '-' && p + 1 < l->len && isdigit(s[p+1]))) {
        int start = p;
        int is_float = 0;
        if (c == '-') p++;
        while (p < l->len && (isdigit(s[p]) || s[p] == '.')) {
            if (s[p] == '.') is_float = 1;
            p++;
        }
        tok.start = s + start;
        tok.len = p - start;
        if (is_float) {
            tok.type = TOK_FLOAT;
            tok.f_val = strtod(tok.start, NULL);
        } else {
            tok.type = TOK_INT;
            tok.i_val = strtoll(tok.start, NULL, 10);
        }
        l->pos = p;
        return tok;
    }

    if (c == '>' && p+1 < l->len && s[p+1] == '=') { tok.type = TOK_GE; l->pos = p+2; return tok; }
    if (c == '<' && p+1 < l->len && s[p+1] == '=') { tok.type = TOK_LE; l->pos = p+2; return tok; }
    if (c == '=' && p+1 < l->len && s[p+1] == '=') { tok.type = TOK_EQ; l->pos = p+2; return tok; }
    if (c == '!' && p+1 < l->len && s[p+1] == '=') { tok.type = TOK_NE; l->pos = p+2; return tok; }
    if (c == '>') { tok.type = TOK_GT; l->pos = p+1; return tok; }
    if (c == '<') { tok.type = TOK_LT; l->pos = p+1; return tok; }
    if (c == '+') { tok.type = TOK_PLUS; l->pos = p+1; return tok; }
    if (c == '-') { tok.type = TOK_MINUS; l->pos = p+1; return tok; }
    if (c == '*') { tok.type = TOK_STAR; l->pos = p+1; return tok; }
    if (c == '/') { tok.type = TOK_SLASH; l->pos = p+1; return tok; }
    if (c == '%') { tok.type = TOK_PERCENT; l->pos = p+1; return tok; }
    if (c == '(') { tok.type = TOK_LPAREN; l->pos = p+1; return tok; }
    if (c == ')') { tok.type = TOK_RPAREN; l->pos = p+1; return tok; }

    if (isalpha(c) || c == '_') {
        int start = p;
        while (p < l->len && (isalnum(s[p]) || s[p] == '_')) p++;
        tok.start = s + start;
        tok.len = p - start;

        if (tok.len == 3 && strncmp(tok.start, "and", 3) == 0) tok.type = TOK_AND;
        else if (tok.len == 2 && strncmp(tok.start, "or", 2) == 0) tok.type = TOK_OR;
        else if (tok.len == 3 && strncmp(tok.start, "not", 3) == 0) tok.type = TOK_NOT;
        else if (tok.len == 4 && strncmp(tok.start, "True", 4) == 0) { tok.type = TOK_BOOL; tok.i_val = 1; }
        else if (tok.len == 5 && strncmp(tok.start, "False", 5) == 0) { tok.type = TOK_BOOL; tok.i_val = 0; }
        else if (tok.len == 4 && strncmp(tok.start, "None", 4) == 0) tok.type = TOK_NONE;
        else tok.type = TOK_NAME;

        l->pos = p;
        return tok;
    }

    tok.type = TOK_ERROR;
    l->pos = p + 1;
    return tok;
}

/* ---- Recursive descent parser → RPN compiler ---- */

static void emit(CompiledExpr *ce, Instr instr) {
    if (ce->len >= ce->cap) {
        ce->cap = ce->cap ? ce->cap * 2 : 32;
        ce->code = realloc(ce->code, sizeof(Instr) * ce->cap);
    }
    ce->code[ce->len++] = instr;
}

static int parse_or(Lexer *l, CompiledExpr *ce);
static int parse_and(Lexer *l, CompiledExpr *ce);
static int parse_not(Lexer *l, CompiledExpr *ce);
static int parse_comparison(Lexer *l, CompiledExpr *ce);
static int parse_additive(Lexer *l, CompiledExpr *ce);
static int parse_multiplicative(Lexer *l, CompiledExpr *ce);
static int parse_primary(Lexer *l, CompiledExpr *ce);

static Token peek_token(Lexer *l) {
    int saved = l->pos;
    Token t = next_token(l);
    l->pos = saved;
    return t;
}

static int parse_or(Lexer *l, CompiledExpr *ce) {
    if (!parse_and(l, ce)) return 0;
    while (1) {
        Token t = peek_token(l);
        if (t.type != TOK_OR) break;
        next_token(l);
        if (!parse_and(l, ce)) return 0;
        Instr instr = {OP_OR}; emit(ce, instr);
    }
    return 1;
}

static int parse_and(Lexer *l, CompiledExpr *ce) {
    if (!parse_not(l, ce)) return 0;
    while (1) {
        Token t = peek_token(l);
        if (t.type != TOK_AND) break;
        next_token(l);
        if (!parse_not(l, ce)) return 0;
        Instr instr = {OP_AND}; emit(ce, instr);
    }
    return 1;
}

static int parse_not(Lexer *l, CompiledExpr *ce) {
    Token t = peek_token(l);
    if (t.type == TOK_NOT) {
        next_token(l);
        if (!parse_not(l, ce)) return 0;
        Instr instr = {OP_NOT}; emit(ce, instr);
        return 1;
    }
    return parse_comparison(l, ce);
}

static int parse_comparison(Lexer *l, CompiledExpr *ce) {
    if (!parse_additive(l, ce)) return 0;
    while (1) {
        Token t = peek_token(l);
        OpCode op;
        if (t.type == TOK_GT) op = OP_GT;
        else if (t.type == TOK_LT) op = OP_LT;
        else if (t.type == TOK_GE) op = OP_GE;
        else if (t.type == TOK_LE) op = OP_LE;
        else if (t.type == TOK_EQ) op = OP_EQ;
        else if (t.type == TOK_NE) op = OP_NE;
        else break;
        next_token(l);
        if (!parse_additive(l, ce)) return 0;
        Instr instr = {op}; emit(ce, instr);
    }
    return 1;
}

static int parse_additive(Lexer *l, CompiledExpr *ce) {
    if (!parse_multiplicative(l, ce)) return 0;
    while (1) {
        Token t = peek_token(l);
        OpCode op;
        if (t.type == TOK_PLUS) op = OP_ADD;
        else if (t.type == TOK_MINUS) op = OP_SUB;
        else break;
        next_token(l);
        if (!parse_multiplicative(l, ce)) return 0;
        Instr instr = {op}; emit(ce, instr);
    }
    return 1;
}

static int parse_multiplicative(Lexer *l, CompiledExpr *ce) {
    if (!parse_primary(l, ce)) return 0;
    while (1) {
        Token t = peek_token(l);
        OpCode op;
        if (t.type == TOK_STAR) op = OP_MUL;
        else if (t.type == TOK_SLASH) op = OP_DIV;
        else if (t.type == TOK_PERCENT) op = OP_MOD;
        else break;
        next_token(l);
        if (!parse_primary(l, ce)) return 0;
        Instr instr = {op}; emit(ce, instr);
    }
    return 1;
}

static int parse_primary(Lexer *l, CompiledExpr *ce) {
    Token t = next_token(l);

    if (t.type == TOK_INT) {
        Instr instr = {OP_CONST_INT}; instr.i_val = t.i_val;
        emit(ce, instr); return 1;
    }
    if (t.type == TOK_FLOAT) {
        Instr instr = {OP_CONST_FLOAT}; instr.f_val = t.f_val;
        emit(ce, instr); return 1;
    }
    if (t.type == TOK_STR) {
        Instr instr = {OP_CONST_STR}; instr.s_val = strndup(t.start, t.len);
        emit(ce, instr); return 1;
    }
    if (t.type == TOK_BOOL) {
        Instr instr = {OP_CONST_BOOL}; instr.i_val = t.i_val;
        emit(ce, instr); return 1;
    }
    if (t.type == TOK_NONE) {
        Instr instr = {OP_CONST_NONE};
        emit(ce, instr); return 1;
    }
    if (t.type == TOK_NAME) {
        Instr instr = {OP_FIELD}; instr.s_val = strndup(t.start, t.len);
        emit(ce, instr); return 1;
    }
    if (t.type == TOK_LPAREN) {
        if (!parse_or(l, ce)) return 0;
        Token closing = next_token(l);
        return closing.type == TOK_RPAREN;
    }
    return 0;
}

/* ---- Evaluator ---- */

static PyObject *eval_compiled(CompiledExpr *ce, PyObject *row) {
    EvalStack stack;
    stack.top = 0;

    for (int i = 0; i < ce->len; i++) {
        Instr *op = &ce->code[i];
        switch (op->op) {
        case OP_CONST_INT: {
            PyObject *v = PyLong_FromLongLong(op->i_val);
            stack_push(&stack, v); Py_DECREF(v); break;
        }
        case OP_CONST_FLOAT: {
            PyObject *v = PyFloat_FromDouble(op->f_val);
            stack_push(&stack, v); Py_DECREF(v); break;
        }
        case OP_CONST_STR: {
            PyObject *v = PyUnicode_FromString(op->s_val);
            stack_push(&stack, v); Py_DECREF(v); break;
        }
        case OP_CONST_BOOL:
            stack_push(&stack, op->i_val ? Py_True : Py_False); break;
        case OP_CONST_NONE:
            stack_push(&stack, Py_None); break;
        case OP_FIELD: {
            PyObject *key = PyUnicode_InternFromString(op->s_val);
            PyObject *val = PyDict_GetItem(row, key);
            Py_DECREF(key);
            stack_push(&stack, val ? val : Py_None);
            break;
        }
        case OP_GT: case OP_LT: case OP_GE: case OP_LE:
        case OP_EQ: case OP_NE: {
            PyObject *right = stack_pop(&stack);
            PyObject *left = stack_pop(&stack);
            if (left == Py_None || right == Py_None) {
                stack_push(&stack, Py_False);
            } else {
                int cmp_op;
                switch (op->op) {
                    case OP_GT: cmp_op = Py_GT; break;
                    case OP_LT: cmp_op = Py_LT; break;
                    case OP_GE: cmp_op = Py_GE; break;
                    case OP_LE: cmp_op = Py_LE; break;
                    case OP_EQ: cmp_op = Py_EQ; break;
                    default:    cmp_op = Py_NE; break;
                }
                PyObject *result = PyObject_RichCompare(left, right, cmp_op);
                if (result) { stack_push(&stack, result); Py_DECREF(result); }
                else { PyErr_Clear(); stack_push(&stack, Py_False); }
            }
            Py_DECREF(left); Py_DECREF(right);
            break;
        }
        case OP_AND: {
            PyObject *right = stack_pop(&stack);
            PyObject *left = stack_pop(&stack);
            stack_push(&stack, (PyObject_IsTrue(left) && PyObject_IsTrue(right)) ? Py_True : Py_False);
            Py_DECREF(left); Py_DECREF(right); break;
        }
        case OP_OR: {
            PyObject *right = stack_pop(&stack);
            PyObject *left = stack_pop(&stack);
            stack_push(&stack, (PyObject_IsTrue(left) || PyObject_IsTrue(right)) ? Py_True : Py_False);
            Py_DECREF(left); Py_DECREF(right); break;
        }
        case OP_NOT: {
            PyObject *val = stack_pop(&stack);
            stack_push(&stack, PyObject_IsTrue(val) ? Py_False : Py_True);
            Py_DECREF(val); break;
        }
        case OP_ADD: case OP_SUB: case OP_MUL: case OP_DIV: case OP_MOD: {
            PyObject *right = stack_pop(&stack);
            PyObject *left = stack_pop(&stack);
            PyObject *result = NULL;
            switch (op->op) {
                case OP_ADD: result = PyNumber_Add(left, right); break;
                case OP_SUB: result = PyNumber_Subtract(left, right); break;
                case OP_MUL: result = PyNumber_Multiply(left, right); break;
                case OP_DIV: result = PyNumber_TrueDivide(left, right); break;
                case OP_MOD: result = PyNumber_Remainder(left, right); break;
                default: break;
            }
            if (result) { stack_push(&stack, result); Py_DECREF(result); }
            else { PyErr_Clear(); stack_push(&stack, Py_None); }
            Py_DECREF(left); Py_DECREF(right); break;
        }
        case OP_END: break;
        }
    }

    if (stack.top > 0) {
        PyObject *result = stack_pop(&stack);
        stack_clear(&stack);
        return result;
    }
    stack_clear(&stack);
    Py_RETURN_NONE;
}

/* ---- Python type wrapper ---- */

typedef struct {
    PyObject_HEAD
    CompiledExpr ce;
    char *expr_str;
} NativeExprObject;

static void NativeExpr_dealloc(NativeExprObject *self) {
    for (int i = 0; i < self->ce.len; i++) {
        if (self->ce.code[i].op == OP_CONST_STR || self->ce.code[i].op == OP_FIELD)
            free(self->ce.code[i].s_val);
    }
    free(self->ce.code);
    free(self->expr_str);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *NativeExpr_call(NativeExprObject *self, PyObject *args, PyObject *kwargs) {
    PyObject *row;
    if (!PyArg_ParseTuple(args, "O!", &PyDict_Type, &row)) return NULL;
    return eval_compiled(&self->ce, row);
}

static PyObject *NativeExpr_repr(NativeExprObject *self) {
    return PyUnicode_FromFormat("<NativeExpr '%s' (%d ops)>", self->expr_str, self->ce.len);
}

static PyTypeObject NativeExprType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "expr_engine.NativeExpr",
    .tp_doc = "Compiled native expression evaluator",
    .tp_basicsize = sizeof(NativeExprObject),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)NativeExpr_dealloc,
    .tp_call = (ternaryfunc)NativeExpr_call,
    .tp_repr = (reprfunc)NativeExpr_repr,
};

/* ---- Module functions ---- */

static PyObject *py_compile_expr(PyObject *self, PyObject *args) {
    const char *expr_str;
    if (!PyArg_ParseTuple(args, "s", &expr_str)) return NULL;

    NativeExprObject *obj = PyObject_New(NativeExprObject, &NativeExprType);
    if (!obj) return NULL;

    obj->ce.code = NULL; obj->ce.len = 0; obj->ce.cap = 0;
    obj->expr_str = strdup(expr_str);

    Lexer lexer = {expr_str, 0, (int)strlen(expr_str)};
    if (!parse_or(&lexer, &obj->ce)) {
        Py_DECREF(obj);
        PyErr_Format(PyExc_ValueError, "Failed to parse expression: '%s'", expr_str);
        return NULL;
    }
    return (PyObject *)obj;
}

static PyObject *py_eval_filter(PyObject *self, PyObject *args) {
    NativeExprObject *expr;
    PyObject *data;
    if (!PyArg_ParseTuple(args, "O!O!", &NativeExprType, &expr, &PyList_Type, &data))
        return NULL;

    Py_ssize_t n = PyList_GET_SIZE(data);
    PyObject *result = PyList_New(0);
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject *row = PyList_GET_ITEM(data, i);
        PyObject *val = eval_compiled(&expr->ce, row);
        if (val && PyObject_IsTrue(val))
            PyList_Append(result, row);
        Py_XDECREF(val);
    }
    return result;
}

static PyObject *py_eval_compute(PyObject *self, PyObject *args) {
    NativeExprObject *expr;
    PyObject *data;
    const char *field_name;
    if (!PyArg_ParseTuple(args, "O!O!s", &NativeExprType, &expr, &PyList_Type, &data, &field_name))
        return NULL;

    PyObject *key = PyUnicode_InternFromString(field_name);
    Py_ssize_t n = PyList_GET_SIZE(data);
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject *row = PyList_GET_ITEM(data, i);
        PyObject *val = eval_compiled(&expr->ce, row);
        if (val) { PyDict_SetItem(row, key, val); Py_DECREF(val); }
    }
    Py_DECREF(key);
    Py_RETURN_NONE;
}

/* ---- Native select: pick fields from list of dicts ---- */

static PyObject *py_native_select(PyObject *self, PyObject *args) {
    PyObject *data, *fields_list;
    if (!PyArg_ParseTuple(args, "O!O!", &PyList_Type, &data, &PyList_Type, &fields_list))
        return NULL;

    Py_ssize_t n = PyList_GET_SIZE(data);
    Py_ssize_t nf = PyList_GET_SIZE(fields_list);

    /* Intern field name strings for fast dict lookup */
    PyObject **field_keys = malloc(sizeof(PyObject*) * nf);
    for (Py_ssize_t j = 0; j < nf; j++) {
        field_keys[j] = PyList_GET_ITEM(fields_list, j);
        Py_INCREF(field_keys[j]);
    }

    PyObject *result = PyList_New(n);
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject *row = PyList_GET_ITEM(data, i);
        PyObject *new_row = PyDict_New();
        for (Py_ssize_t j = 0; j < nf; j++) {
            PyObject *val = PyDict_GetItem(row, field_keys[j]);
            if (val) {
                PyDict_SetItem(new_row, field_keys[j], val);
            } else {
                PyDict_SetItem(new_row, field_keys[j], Py_None);
            }
        }
        PyList_SET_ITEM(result, i, new_row); /* steals ref */
    }

    for (Py_ssize_t j = 0; j < nf; j++) Py_DECREF(field_keys[j]);
    free(field_keys);
    return result;
}

/* ---- Native dedupe: remove duplicates by key fields ---- */

static PyObject *py_native_dedupe(PyObject *self, PyObject *args) {
    PyObject *data, *key_fields;
    if (!PyArg_ParseTuple(args, "O!O!", &PyList_Type, &data, &PyList_Type, &key_fields))
        return NULL;

    Py_ssize_t n = PyList_GET_SIZE(data);
    Py_ssize_t nk = PyList_GET_SIZE(key_fields);

    PyObject *seen = PySet_New(NULL);
    PyObject *result = PyList_New(0);

    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject *row = PyList_GET_ITEM(data, i);

        /* Build key tuple */
        PyObject *key_tuple = PyTuple_New(nk);
        for (Py_ssize_t j = 0; j < nk; j++) {
            PyObject *field = PyList_GET_ITEM(key_fields, j);
            PyObject *val = PyDict_GetItem(row, field);
            if (!val) val = Py_None;
            Py_INCREF(val);
            PyTuple_SET_ITEM(key_tuple, j, val);
        }

        if (!PySet_Contains(seen, key_tuple)) {
            PySet_Add(seen, key_tuple);
            PyList_Append(result, row);
        }
        Py_DECREF(key_tuple);
    }

    Py_DECREF(seen);
    return result;
}

/* ---- Native sort: Schwartzian transform — extract keys to C doubles ---- */

/* Sortable entry: extracted key + original index */
typedef struct {
    double key;
    int is_none;  /* 1 = None/missing, pushed to end */
    Py_ssize_t idx;
} SortEntry;

static int _sort_desc_flag = 0;

static int sort_entry_compare(const void *a, const void *b) {
    const SortEntry *ea = (const SortEntry *)a;
    const SortEntry *eb = (const SortEntry *)b;

    /* None values always sort last */
    if (ea->is_none && eb->is_none) return 0;
    if (ea->is_none) return 1;
    if (eb->is_none) return -1;

    /* Pure C double comparison — no Python API calls */
    int result;
    if (ea->key < eb->key) result = -1;
    else if (ea->key > eb->key) result = 1;
    else result = 0;

    return _sort_desc_flag ? -result : result;
}

static PyObject *py_native_sort(PyObject *self, PyObject *args) {
    PyObject *data;
    const char *field;
    int descending = 0;

    if (!PyArg_ParseTuple(args, "O!s|p", &PyList_Type, &data, &field, &descending))
        return NULL;

    Py_ssize_t n = PyList_GET_SIZE(data);
    if (n <= 1) {
        Py_INCREF(data);
        return data;
    }

    /* Phase 1: Extract keys into C array (one pass through Python API) */
    PyObject *field_key = PyUnicode_InternFromString(field);
    SortEntry *entries = malloc(sizeof(SortEntry) * n);

    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject *row = PyList_GET_ITEM(data, i);
        PyObject *val = PyDict_GetItem(row, field_key);
        entries[i].idx = i;

        if (!val || val == Py_None) {
            entries[i].is_none = 1;
            entries[i].key = 0.0;
        } else if (PyFloat_Check(val)) {
            entries[i].is_none = 0;
            entries[i].key = PyFloat_AS_DOUBLE(val);
        } else if (PyLong_Check(val)) {
            entries[i].is_none = 0;
            entries[i].key = PyLong_AsDouble(val);
        } else {
            /* Non-numeric: hash for consistent ordering */
            entries[i].is_none = 0;
            entries[i].key = (double)PyObject_Hash(val);
        }
    }
    Py_DECREF(field_key);

    /* Phase 2: Pure C sort — no Python API calls during comparison */
    _sort_desc_flag = descending;
    qsort(entries, n, sizeof(SortEntry), sort_entry_compare);

    /* Phase 3: Build result list from sorted indices */
    PyObject *result = PyList_New(n);
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject *row = PyList_GET_ITEM(data, entries[i].idx);
        Py_INCREF(row);
        PyList_SET_ITEM(result, i, row);
    }
    free(entries);
    return result;
}

/* ---- Module definition ---- */

static PyMethodDef module_methods[] = {
    {"compile_expr",  py_compile_expr,  METH_VARARGS, "Compile expression to native evaluator"},
    {"eval_filter",   py_eval_filter,   METH_VARARGS, "Filter list of dicts by compiled expression"},
    {"eval_compute",  py_eval_compute,  METH_VARARGS, "Compute field on list of dicts"},
    {"native_select", py_native_select, METH_VARARGS, "Select fields from list of dicts (C speed)"},
    {"native_dedupe", py_native_dedupe, METH_VARARGS, "Deduplicate list of dicts by key fields (C speed)"},
    {"native_sort",   py_native_sort,   METH_VARARGS, "Sort list of dicts by field (C speed)"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef module_def = {
    PyModuleDef_HEAD_INIT,
    "expr_engine",
    "Native C expression engine for Blitz — filter, compute, select, dedupe, sort",
    -1,
    module_methods
};

PyMODINIT_FUNC PyInit_expr_engine(void) {
    if (PyType_Ready(&NativeExprType) < 0) return NULL;
    PyObject *m = PyModule_Create(&module_def);
    if (!m) return NULL;
    Py_INCREF(&NativeExprType);
    PyModule_AddObject(m, "NativeExpr", (PyObject *)&NativeExprType);
    return m;
}

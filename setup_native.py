"""Build the native C expression engine for Blitz."""
from setuptools import setup, Extension

ext = Extension(
    "blitz.native.expr_engine",
    sources=["blitz/native/expr_engine.c"],
    extra_compile_args=["-O3", "-march=native", "-flto"],
    extra_link_args=["-flto"],
)

setup(
    name="blitz-native",
    ext_modules=[ext],
)

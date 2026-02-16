"""Build the native C expression engine for BlitzTigerClaw."""
from setuptools import setup, Extension

ext = Extension(
    "blitztigerclaw.native.expr_engine",
    sources=["blitztigerclaw/native/expr_engine.c"],
    extra_compile_args=["-O3", "-march=native", "-flto"],
    extra_link_args=["-flto"],
)

setup(
    name="blitztigerclaw-native",
    ext_modules=[ext],
)

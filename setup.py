#coding: utf8

"""
Setup script for pscache.
"""

from glob import glob


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(name='pscache',
      version='0.0.1',
      author="TJ Lane",
      author_email="tjlane@stanford.edu",
      description='CSPad geometry, assembly and optimization',
      packages=["pscache"],
      package_dir={"pscache": "pscache"},
      scripts=[s for s in glob('scripts/*') if not s.endswith('__.py')],
      test_suite="test")


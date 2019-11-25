# -*- coding: utf-8 -*-
import os

from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.readlines()

with open('README.md') as f:
    README = f.read()

setup(name='tqos_asr_spacy_agent',
      version='0.1.0',
      description='Apply spacy analysis to paragraphs',
      long_description=README,
      long_description_content_type="text/markdown",
      classifiers=[
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "License :: OSI Approved :: GNU Affero General Public License v3",
      ],
      author='Marc-Antoine Parent',
      author_email='maparent@conversence.com',
      project_urls={
          'Source': 'https://github.com/KnowledgeGarden/tqos-asr-spacy-agents.git'
      },
      license='AGPLv3',
      packages=find_packages(),
      zip_safe=False,
      setup_requires=['pip>=6'],
      install_requires=requirements,
      entry_points={
          "console_scripts": [
              "tqos_asr_spacy_agent = tqos_asr_spacy_agent:main"
          ],
      },
      )

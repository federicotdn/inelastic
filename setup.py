from distutils.core import setup

VERSION = '0.1.1'

with open('requirements.txt') as f:
    requires = f.read().splitlines()

with open('README.md') as f:
    long_description = f.read()

setup(
    name='inelastic',
    py_modules=['inelastic'],
    version=VERSION,
    description='Print an Elasticsearch inverted index as a CSV table or JSON object.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Federico Tedin',
    author_email='federicotedin@gmail.com',
    install_requires=requires,
    python_requires='>=3',
    url='https://github.com/federicotdn/inelastic',
    download_url='https://github.com/federicotdn/inelastic/archive/{}.tar.gz'.format(VERSION),
    keywords=['elasticsearch', 'index', 'inverted', 'json', 'csv', 'elastic'],
    license='Apache Software License',
    classifiers=[
        'Programming Language :: Python :: 3 :: Only',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Utilities',
        'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
        'Topic :: Text Processing :: Indexing'
    ],
    entry_points={
        'console_scripts': [
            'inelastic=inelastic:main'
        ]
    }
)

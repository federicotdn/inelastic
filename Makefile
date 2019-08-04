flake8:
	flake8 inelastic.py

package:
	mkdir -p dist
	rm -rf dist/*
	python3 setup.py sdist

upload: package
	pip install twine
	twine upload dist/*

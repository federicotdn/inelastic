lint:
	flake8 inelastic.py
	black --check inelastic.py

format:
	black inelastic.py

package:
	mkdir -p dist
	rm -rf dist/*
	python3 setup.py sdist

upload: package
	pip install twine
	twine upload dist/*

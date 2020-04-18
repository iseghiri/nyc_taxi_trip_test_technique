clean: clean-build

clean-build:
	rm -fr dist/

build:
	mkdir ./dist
	cp ./src/main.py ./dist
	cp -r ./src/data ./dist
	cd ./src && zip -x main.py -r ../dist/jobs.zip .
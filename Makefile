clean: clean-build

clean-build:
	rm -fr dist/

build:
	mkdir ./dist
	mkdir ./dist/outputs
	cp ./src/main.py ./dist
	cp -r ./src/data ./dist
	cd ./src && zip -x main.py -r ../dist/jobs.zip .
	
	
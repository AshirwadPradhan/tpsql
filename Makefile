ifeq ($(OS),Windows_NT)
ROOT_D := $(shell cd)
systest:
	@echo Windows
clean:
	-rmdir /s /q $(ROOT_D)\src\out
	-rmdir /s /q $(ROOT_D)\src\spark-warehouse
	-rmdir /s /q $(ROOT_D)\src\tmp
	-rmdir /s /q $(ROOT_D)\src\__pycache__
init:
	-mkdir $(ROOT_D)\src\out
	-mkdir $(ROOT_D)\src\tmp
install:
	pip install --user -r requirements.txt
run-dw:
	@echo Pass PORT as argument to Makefile command
	python src\tddw.py -p $(PORT) 
run-agg:
	python src\aggregator.py
run-client:
	python src\client.py
prepare: init install

else
ROOT_L := $(shell pwd)
systest:
	@echo UNAME_S
clean:
	-rm -rf $(ROOT_L)/src/out
	-rm -rf $(ROOT_L)/src/spark-warehouse
	-rm -rf $(ROOT_L)/src/tmp
	-rm -rf $(ROOT_L)/src/__pycache__
init:
	-mkdir $(ROOT_L)/src/out
	-mkdir $(ROOT_L)/src/tmp
install:
	pip3 install -r requirements.txt
run-dw:
	@echo Pass PORT as argument to Makefile command
	python3 src/tddw.py -p $(PORT)
run-agg:
	python3 src/aggregator.py
run-client:
	python3 src/client.py
prepare: init install
endif
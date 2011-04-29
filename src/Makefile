
all: main
#svd.so

svd.so: svd.pyx
	python setup.py build_ext -i

clean:
	rm -f svd.so svd.c

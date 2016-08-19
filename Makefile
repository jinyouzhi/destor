SRC := ./src
USR_SUB_DIR := $(SRC)/index $(SRC)/chunking $(SRC)/recipe $(SRC)/storage $(SRC)/fsl $(SRC)/utils $(SRC) $(SRC)/demo

default: subdir
subdir:
	@for n in $(USR_SUB_DIR); do $(MAKE) -C $$n ; done
	cp -r ./src/demo/bin ./

init_sys:
	rm -f ./bin/backup.log
	rm -f ./bin/recipes/*.*
	rm -f ./bin/containers/*.*

  
clean:
	@for n in $(USR_SUB_DIR); do $(MAKE) -C $$n clean; done

SRC_DIR := ./
BUILD_DIR := ./bin
#INCLUDE_DIR := -I/opt/local/include/glib-2.0 -I/opt/local/lib/glib-2.0/include

LIB_DIR := ../lib
LIB := -lglib-2.0 -ldestor -lchunk -lindex -lrecipe -lstorage -lutils -lfsl -lm -lcrypto -lpthread

CC := gcc

CFLAG :=  -g
LFLAG := -L$(LIB_DIR) $(LIB) 


CFLAG += `pkg-config --cflags glib-2.0`
LFLAG += `pkg-config --libs glib-2.0`

.PHONY:all target $(BUILD_DIR)

all: target

target: $(BUILD_DIR) $(OBJ) 


$(BUILD_DIR):
	mkdir -p $@
	$(CC) destor.c $(CFLAG) $(LFLAG) -o $(BUILD_DIR)/demo $(INCLUDE_DIR)


clean:
	rm -rf $(BUILD_DIR) 
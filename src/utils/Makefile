SRC_DIR := ./
BUILD_DIR := ./obj_dir
#INCLUDE_DIR := -I/opt/local/include/glib-2.0 -I/opt/local/lib/glib-2.0/include

LIB_DIR := ../lib/
LIB := -lglib-2.0

CC := gcc
AR := ar

CFLAG :=  -g
LFLAG :=  
ARFLAG := -rcs


CFLAG += `pkg-config --cflags glib-2.0`
LFLAG += `pkg-config --libs glib-2.0`


SRC := $(wildcard $(SRC_DIR)/*.c)
OBJ := $(patsubst %.c,%.o, $(subst $(SRC_DIR),$(BUILD_DIR), $(SRC)))

ARCHIVE := libutils.a

.PHONY:all target $(BUILD_DIR)

all: target

target: $(BUILD_DIR) $(OBJ) $(ARCHIVE)


$(BUILD_DIR):
	mkdir -p $@

$(BUILD_DIR)/%.o:$(SRC_DIR)/%.c
	$(CC) -c $< $(CFLAG) -o $@ $(INCLUDE_DIR)

$(ARCHIVE):$(OBJ)
	$(AR) $(ARFLAG) $@ $(OBJ)
	ranlib $@
	mv -f $@ $(LIB_DIR)

clean:
	rm -rf $(BUILD_DIR) $(TESTBIN_DIR) $(ARCHIVE)
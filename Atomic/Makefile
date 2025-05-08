CC=g++
CXX=g++
LD=g++

EXESRC=atomicdemo.cpp atomicdemo_multivar.cpp
EXEOBJ=$(EXESRC:.cpp=.o)

INCS=-I.
CFLAGS = -Wall -std=c++14 -g $(INCS)
CXXFLAGS = -Wall -std=c++14 -g $(INCS)
LDFLAGS = -pthread

EXE = atomicdemo
EXE_MULTIVAR = atomicdemo_multivar
TARGETS = $(EXE) $(EXE_MULTIVAR)

TAR=tar
TARFLAGS=-cvf
TARNAME=atomicdemo.tar
TARSRCS=$(EXESRC) Makefile README

all: $(TARGETS)

clean:
	$(RM) $(TARGETS) $(EXE) $(OBJ) $(EXEOBJ) *~ *core

depend:
	makedepend -- $(CFLAGS) -- $(SRC) $(LIBSRC)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)


CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

main: y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o RelOp.o Statistics.o main.o
	$(CC) -o main y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o RelOp.o Statistics.o main.o -lfl -lpthread


gtest: y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o RelOp.o Statistics.o gtest-all.o gtest.o
	$(CC) -o gtest y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o RelOp.o Statistics.o gtest-all.o gtest.o -lfl -lpthread
#
#
	
#gtest-all.o: gtest-all.cc
#	$(CC)  -g -DGTEST_HAS_PTHREAD=0 -c gtest-all.cc
gtest-all.o: gtest-all.cc
	$(CC)  -g -c gtest-all.cc
#-DGTEST_HAS_PTHREAD=0

#test: Statistics.o RelationHelper.o AttributeHelper.o y.tab.o lex.yy.o test.o
#	$(CC) -o test Statistics.o RelationHelper.o AttributeHelper.o y.tab.o lex.yy.o test.o -lfl -lpthread




main.o : main.cc
	$(CC) -g -c main.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c y.tab.c
	
yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c yyfunc.tab.c


lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c


lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c


clean:
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h

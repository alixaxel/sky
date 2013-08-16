//line parser.y:2
package query

import __yyfmt__ "fmt"

//line parser.y:3
import (
	"bufio"
	"bytes"
	"io"
)

//line parser.y:13
type yySymType struct {
	yys              int
	token            int
	str              string
	query            *Query
	statement        QueryStep
	statements       QueryStepList
	selection        *Selection
	selection_field  *SelectionField
	selection_fields []*SelectionField
}

const TSELECT = 57346
const TSEMICOLON = 57347
const TCOMMA = 57348
const TLPAREN = 57349
const TRPAREN = 57350
const TIDENT = 57351

var yyToknames = []string{
	"TSELECT",
	"TSEMICOLON",
	"TCOMMA",
	"TLPAREN",
	"TRPAREN",
	"TIDENT",
}
var yyStatenames = []string{}

const yyEofCode = 1
const yyErrCode = 2
const yyMaxDepth = 200

//line parser.y:101

type Parser struct {
}

func NewParser() *Parser {
	return &Parser{}
}

func (p *Parser) Parse(r io.Reader) *Query {
	l := newLexer(bufio.NewReader(r))
	yyParse(l)
	return l.query
}

func (p *Parser) ParseString(s string) *Query {
	return p.Parse(bytes.NewBufferString(s))
}

//line yacctab:1
var yyExca = []int{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyNprod = 12
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 17

var yyAct = []int{

	9, 15, 16, 10, 17, 13, 12, 3, 7, 6,
	5, 1, 8, 14, 11, 2, 4,
}
var yyPact = []int{

	6, -1000, 4, 3, -1000, -6, 6, -1000, 0, -1000,
	-2, -1000, -6, -7, -1000, -1000, -4, -1000,
}
var yyPgo = []int{

	0, 16, 7, 15, 0, 12, 11,
}
var yyR1 = []int{

	0, 6, 3, 3, 3, 2, 1, 5, 5, 5,
	4, 4,
}
var yyR2 = []int{

	0, 1, 0, 2, 3, 1, 2, 0, 1, 3,
	3, 4,
}
var yyChk = []int{

	-1000, -6, -3, -2, -1, 4, 5, 5, -5, -4,
	9, -2, 6, 7, -4, 8, 9, 8,
}
var yyDef = []int{

	2, -2, 1, 0, 5, 7, 0, 3, 6, 8,
	0, 4, 0, 0, 9, 10, 0, 11,
}
var yyTok1 = []int{

	1,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9,
}
var yyTok3 = []int{
	0,
}

//line yaccpar:1

/*	parser for yacc output	*/

var yyDebug = 0

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

const yyFlag = -1000

func yyTokname(c int) string {
	// 4 is TOKSTART above
	if c >= 4 && c-4 < len(yyToknames) {
		if yyToknames[c-4] != "" {
			return yyToknames[c-4]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yylex1(lex yyLexer, lval *yySymType) int {
	c := 0
	char := lex.Lex(lval)
	if char <= 0 {
		c = yyTok1[0]
		goto out
	}
	if char < len(yyTok1) {
		c = yyTok1[char]
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			c = yyTok2[char-yyPrivate]
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		c = yyTok3[i+0]
		if c == char {
			c = yyTok3[i+1]
			goto out
		}
	}

out:
	if c == 0 {
		c = yyTok2[1] /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %U %s\n", uint(char), yyTokname(c))
	}
	return c
}

func yyParse(yylex yyLexer) int {
	var yyn int
	var yylval yySymType
	var yyVAL yySymType
	yyS := make([]yySymType, yyMaxDepth)

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yychar := -1
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yychar), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = yyPact[yystate]
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yychar < 0 {
		yychar = yylex1(yylex, &yylval)
	}
	yyn += yychar
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yychar { /* valid shift */
		yychar = -1
		yyVAL = yylval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = yyDef[yystate]
	if yyn == -2 {
		if yychar < 0 {
			yychar = yylex1(yylex, &yylval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && yyExca[xi+1] == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = yyExca[xi+0]
			if yyn < 0 || yyn == yychar {
				break
			}
		}
		yyn = yyExca[xi+1]
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error("syntax error")
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf("saw %s\n", yyTokname(yychar))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = yyPact[yyS[yyp].yys] + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = yyAct[yyn] /* simulate a shift of "error" */
					if yyChk[yystate] == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yychar))
			}
			if yychar == yyEofCode {
				goto ret1
			}
			yychar = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= yyR2[yyn]
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = yyR1[yyn]
	yyg := yyPgo[yyn]
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = yyAct[yyg]
	} else {
		yystate = yyAct[yyj]
		if yyChk[yystate] != -yyn {
			yystate = yyAct[yyg]
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 1:
		//line parser.y:39
		{
			l := yylex.(*yylexer)
			l.query.Steps = yyS[yypt-0].statements
		}
	case 2:
		//line parser.y:47
		{
			yyVAL.statements = make(QueryStepList, 0)
		}
	case 3:
		//line parser.y:51
		{
			yyVAL.statements = make(QueryStepList, 0)
			yyVAL.statements = append(yyVAL.statements, yyS[yypt-1].statement)
		}
	case 4:
		//line parser.y:56
		{
			yyVAL.statements = append(yyS[yypt-2].statements, yyS[yypt-0].statement)
		}
	case 5:
		//line parser.y:62
		{
			yyVAL.statement = QueryStep(yyS[yypt-0].selection)
		}
	case 6:
		//line parser.y:67
		{
			l := yylex.(*yylexer)
			yyVAL.selection = NewSelection(l.query)
			yyVAL.selection.Fields = yyS[yypt-0].selection_fields
		}
	case 7:
		//line parser.y:76
		{
			yyVAL.selection_fields = make([]*SelectionField, 0)
		}
	case 8:
		//line parser.y:80
		{
			yyVAL.selection_fields = make([]*SelectionField, 0)
			yyVAL.selection_fields = append(yyVAL.selection_fields, yyS[yypt-0].selection_field)
		}
	case 9:
		//line parser.y:85
		{
			yyVAL.selection_fields = append(yyS[yypt-2].selection_fields, yyS[yypt-0].selection_field)
		}
	case 10:
		//line parser.y:92
		{
			yyVAL.selection_field = NewSelectionField("", yyS[yypt-2].str)
		}
	case 11:
		//line parser.y:96
		{
			yyVAL.selection_field = NewSelectionField(yyS[yypt-1].str, yyS[yypt-3].str)
		}
	}
	goto yystack /* stack new state and value */
}

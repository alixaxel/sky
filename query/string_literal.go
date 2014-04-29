package query

import (
	"strconv"

	"github.com/skydb/sky/db"
)

type StringLiteral struct {
	queryElementImpl
	value string
}

func (l *StringLiteral) VarRefs() []*VarRef {
	return []*VarRef{}
}

func (l *StringLiteral) Codegen() (string, error) {
	// Retrieve the opposite side of the binary expression this literal is a part of.
	var opposite Expression
	if parent, ok := l.Parent().(*BinaryExpression); ok {
		if parent.Lhs() == l {
			opposite = parent.Rhs()
		} else if parent.Rhs() == l {
			opposite = parent.Lhs()
		}
	}

	// If the string is a part of a binary expression where the other side
	// is a factor variable reference then we have to convert this value to
	// a factor.
	if ref, ok := opposite.(*VarRef); ok {
		query := l.Query()
		variable, err := ref.Variable()
		if err != nil {
			return "", err
		}
		if variable.DataType == db.Factor {
			targetName := variable.Name
			if variable.Association != "" {
				targetName = variable.Association
			}
			p, _ := query.Tx.Property(targetName)
			sequence, _ := query.Tx.Factorize(p.ID, l.value)
			return strconv.FormatUint(uint64(sequence), 10), nil
		}
	}

	return l.String(), nil
}

func (l *StringLiteral) String() string {
	return strconv.Quote(l.value)
}

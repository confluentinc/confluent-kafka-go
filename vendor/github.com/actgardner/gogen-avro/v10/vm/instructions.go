package vm

import (
	"fmt"
)

// The value of NoopField as the operand signifies the operand is unused.
const NoopField = 65535

// Constants for the data types supported by the Read and Set operations.
// If the value is > 9 it's assumed to be the length of a Fixed type.
const (
	Unused int = iota
	Null
	Boolean
	Int
	Long
	Float
	Double
	Bytes
	String
	UnionElem
	UnusedLong
)

// Represents a single VM instruction consisting of an opcode and 0 or 1 operands.
type Instruction struct {
	Op      Op
	Operand int
}

func (i Instruction) String() string {
	if i.Op == Read || i.Op == Set {
		switch i.Operand {
		case 0:
			return fmt.Sprintf("%v(unused)", i.Op)
		case 1:
			return fmt.Sprintf("%v(null)", i.Op)
		case 2:
			return fmt.Sprintf("%v(boolean)", i.Op)
		case 3:
			return fmt.Sprintf("%v(int)", i.Op)
		case 4:
			return fmt.Sprintf("%v(long)", i.Op)
		case 5:
			return fmt.Sprintf("%v(float)", i.Op)
		case 6:
			return fmt.Sprintf("%v(double)", i.Op)
		case 7:
			return fmt.Sprintf("%v(bytes)", i.Op)
		case 8:
			return fmt.Sprintf("%v(string)", i.Op)
		case 9:
			return fmt.Sprintf("%v(union)", i.Op)
		case 10:
			return fmt.Sprintf("%v(UnusedLong)", i.Op)
		}
	}
	if i.Operand == NoopField {
		return fmt.Sprintf("%v()", i.Op)
	}
	return fmt.Sprintf("%v(%v)", i.Op, i.Operand)
}
